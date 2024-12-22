import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')

exchange = pd.read_csv('ex_rates.csv')

"""
Первый этап: Создание ранжированного списка провайдеров по методу TOPSIS:
Методика проведения: 
* Формирование матрицы решений: строки - альтернативы, столбцы — критерии; 
* Нормализация матрицы решений; 
* Взвешивание нормализованных значений, т.е. присвоение весов каждому критерию; 
* Определение идеального и противоположного идеальному решений; 
* Расчет относительной близости альтернатив к идеальному решению, 
ранжирование альтернатив по принципу близости к фиктивной идеальной альтернативе.
"""

# Шаг 1: Создание матрицы показателей провайдеров
def normalize_matrix(providers, criterions, weights):
    """
    Создает матрицу характеристик провайдеров,
    в качестве аргументов принимает признаки и веса к ним.
    """
    matrix = providers[criterions].to_numpy()
    norm_matrix = matrix / np.sqrt((matrix**2).sum(axis=0))
    weighted_matrix = norm_matrix * weights
    return weighted_matrix


# Шаг 2: Определение лучшего и худшего провайдера
def calculate_solutions(matrix, criterion_types):
    """
    Вычисляет наилучшее и наихудшее решения.
    matrix - Взвешенная матрица критериев провайдеров
    criterion_types - максимизация или минимизация значения критерия для определения стратегии "лучше"
    Возвращаемое значение - значения критериев лучшего и худшего провайдеров.
    """
    good = []
    bad = []
    for i, mark in enumerate(criterion_types):
        if mark == 'max':
            good.append(matrix[:, i].max())
            bad.append(matrix[:, i].min())
        elif mark == 'min':
            good.append(matrix[:, i].min())
            bad.append(matrix[:, i].max())
    return np.array(good), np.array(bad)

# Шаг 3: Вычисляем коэффициент близости
def calculate_distances(matrix, good, bad):
    """
    Вычисляет расстояния до идеального и анти-идеального решений.
    """
    dist_to_good = np.sqrt(((matrix - good)**2).sum(axis=1))
    dist_to_bad = np.sqrt(((matrix - bad)**2).sum(axis=1))
    coef = dist_to_bad / (dist_to_good + dist_to_bad)
    return coef

# Шаг 4: Ранжирование и выбор провайдера
def rank_providers(providers, coef):
    """
    Ранжирует провайдеров по найденным коэффициентам и возвращает упорядоченный список.
    """
    providers['topsis_score'] = coef
    ranked_providers = providers.sort_values(by='topsis_score', ascending=False)
    return ranked_providers

# Шаг 5: TOPSIS
def topsis(providers, criterions, weights, criterion_types):
    provider_matrix = normalize_matrix(providers, criterions, weights)
    good_provider, bad_provider = calculate_solutions(provider_matrix, criterion_types)
    # Расчет коэффициента близости
    score = calculate_distances (provider_matrix, good_provider, bad_provider)
    # Ранжирование провайдеров
    ranked_providers = rank_providers(providers, score)

    return ranked_providers

"""
Второй этап: Оптимизация маршрутизации исполнения транзакции.
Создаем функциол, который учтет лимиты, наложенные провайдерами.
Создаем конвейер из оптимальных провайдеров для каждой транзакции.
"""

def optimize_routes(payments, providers, currency_rates):
    # Формирует поле flow, которое будет содержать конвейер из провайдеров.
    payments['flow'] = ''
    payments['Profit'] = 0  # оборот за вычетом комиссии
    payments['Processing_Time'] = 0  # общее время обработки платежа
    payments['Successful provider'] = 0  # !!! Провайдер, на котором CAPTURED

    providers['TIME'] = pd.to_datetime(providers['TIME'])

    # Обновляем суточные лимиты по первым значениям на дату
    providers['DATE'] = providers['TIME'].dt.date
    providers['LIMIT_MAX'] = providers.groupby(['ID'])['LIMIT_MAX'].transform('first')
    providers['LIMIT_MIN'] = providers.groupby(['ID'])['LIMIT_MIN'].transform('first')

    providers = providers.merge(currency_rates, left_on='CURRENCY', right_on='destination', how='left')
    payments = payments.merge(currency_rates, left_on='cur', right_on='destination', how='left')

    # Конвертируем валюты для всех денежных ограничений
    convert_columns = ['MIN_SUM', 'MAX_SUM', 'LIMIT_MIN', 'LIMIT_MAX']
    for column in convert_columns:
        providers[column] *= providers['rate']

    # Инициализируем дневные суммы на провайдерах
    provider_daily_sums = {provider_id: 0 for provider_id in providers['ID']}
    
    # обработка транзакций
    for i, payment in payments.iterrows():
        amount = payment['amount'] * payment['rate']
        payment_time = pd.to_datetime(payment['eventTimeRes'])
        currency = payment['cur']
        route = []
        total_commission = 0
        total_processing_time = 0
        is_first = True  # Флаг для обновления комиссии

        # Ранжируем провайдеров по методу TOPSIS
        criterions = ['CONVERSION', 'COMMISSION', 'AVG_TIME']
        weights = [0.2, 0.6, 0.2]  # задаются пока вручную, необходимо экспертное мнение для определения значимости критериев
        marks = ['max', 'min', 'min']
        filtered_providers = providers[(providers['TIME'] <= payment_time) & (providers['CURRENCY'] == currency)]
        filtered_providers = filtered_providers.loc[filtered_providers.groupby('ID')['TIME'].idxmax()]
        ranked_providers = topsis(filtered_providers, criterions, weights, marks)

        # Фильтруем и сортируем ранжированный список провайдеров, актуальных на время платежа
        sorted_providers = ranked_providers[(ranked_providers['TIME'] <= payment_time)].sort_values(
            by='topsis_score', ascending=False) 

        # Пытаемся обработать платеж, переходя от одного провайдера к другому
        for _, provider in sorted_providers.iterrows():
            provider_id = provider['ID']

            # Условия доступности провайдера
            if amount < provider['MIN_SUM'] or amount > provider['MAX_SUM']:
                continue  # Пропускаем провайдера, если сумма не в пределах MIN_SUM и MAX_SUM
            if provider_daily_sums[provider_id] >= provider['LIMIT_MAX']:
                continue  # Пропускаем провайдера, если достигнут дневной лимит

            # Проверка возможности обработки платежа провайдером на основе его конверсии
            conversion_success = np.random.choice(
                [True, False], p=[provider['CONVERSION'], 1 - provider['CONVERSION']]
            )
            if not conversion_success:
                if is_first: total_processing_time += provider['AVG_TIME']  # Учитываем время при неуспехе
                route.append(str(provider_id))
                continue  # Если ошибка конверсии, переходим к следующему провайдеру

            # Рассчитываем сумму, которую может обработать провайдер
            available_limit = provider['LIMIT_MAX'] - provider_daily_sums[provider_id]
            # В случае переполнения LIMIT_MAX операция не будет выполнена
            if available_limit < amount:
                if is_first: total_processing_time += provider['AVG_TIME']  # Учитываем время при неуспехе
                continue
            commission = amount * provider['COMMISSION']
            route.append(str(provider_id))
            # Симулируем процесс
            if is_first:
                payments.at[i, 'Successful provider'] = provider_id
                provider_daily_sums[provider_id] += amount
                total_commission += commission
                total_processing_time += provider['AVG_TIME']
                is_first = False

        payments.at[i, 'flow'] = '-'.join(route)
        if payments.at[
            i, 'flow'] != '':  # Профит (оборот - комиссия)
            payments.at[i, 'Profit'] = amount - total_commission
        payments.at[i, 'Processing_Time'] = total_processing_time

    # Считаем за день штрафы за недобор необходимого минимума
    limit_penalty_used = 0
    limit_penalty_all = 0
    for provider_id, daily_sum in provider_daily_sums.items():
        limit_min = providers[providers['ID'] == provider_id]['LIMIT_MIN'].iloc[0].item()
        if limit_min > daily_sum:
            limit_penalty_all += 0.01 * (limit_min - daily_sum)
        if daily_sum > 0 and limit_min > daily_sum:
            pen = 0.01 * (limit_min - daily_sum)
            limit_penalty_used += pen
    return (payments, limit_penalty_used, limit_penalty_all)

"""
Третий этап: Аккумулируем результаты работы алгоритма платежного конвейера за день
"""

def process_transactions(transactions_name, providers_name, exchange_name='ex_rates.csv'):
    '''
    transactions_name - путь к файлу с транзакциями
    providers_name - путь к файлу с провайдерами
    exchange_name - путь к файлу с курсом валют

    Return:
    1. Среднее время
    2. Общий профит (оборот за вычетом комиссии)
    3. Общая сумма штрафа за недобор суточного лимита только использованных в эти сутки провайдеров
    4. Общая сумма штрафа за недобор суточного лимита всех провайдеров
    '''
    trans = pd.read_csv(transactions_name)[:10000]
    prov = pd.read_csv(providers_name)[:10000]
    exchange = pd.read_csv(exchange_name)

    optimized_payments, general_limit_penalty_used, general_limit_penalty_all = optimize_routes(trans, prov, exchange)

    time_mean = optimized_payments[optimized_payments['flow'] != '']['Processing_Time'].mean()
    profit = optimized_payments['Profit'].sum()
    optimized_payments = optimized_payments.drop(
        columns=['Profit', 'Processing_Time', 'Successful provider', 'rate', 'destination'])
    optimized_payments.to_csv('optimized_payments.csv', index=False)
    return (time_mean, profit, general_limit_penalty_used, general_limit_penalty_all)

# рассчитываем показатели по итогу дня
time_mean, profit, penalty_used, penalty_all = process_transactions('payments_1.csv', 'providers_1.csv')

print(f"Результаты в долларах:")
print(f"Среднее время = {time_mean}")
print(f"Профит = {profit}")
print(f"Штраф за недобор только использованных провайдеров = {penalty_used}")
print(f"Штраф за недобор всех провайдеров = {penalty_all}")