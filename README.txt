Как запустить докер:
docker pull pzof/1team:latest

docker run -it --name pzof_1team \ 
    -v <Полный путь до папки с данными>:/data \
    -v /<Полный путь до папки куда планируется вывод>:/app/output \
    pzof/1team python 1TEAM_XMAS_HACK.py --data-paths /data/<название файла с платежами>.csv /data/<название файла с провайдерами>.csv /data/<название файла с курсом валют>.csv

команда для переноса файла с выводом из докера в локальную директорию docker cp pzof_1team:/app/output/optimized_payments.csv /path/to/local/directory/


