Даг csv_to_postgresql выполняет загрузку данных в таблицы rd.deal_info и rd.product

В качестве стратегии загрузки была выбрана insert...if not exists для таблицы deal_info, так как данные из csv и таблицы не пересекаются по столбцам effective_from_date, effective_to_date
В качестве столбцов, идентифицирующих записи, были определены столбцы ("deal_rk", "effective_from_date") для deal_info

В качестве стратегии загрузки была выбрана truncate & insert для таблицы product, так как данные из csv и таблицы пересекаются по столбцам effective_from_date, effective_to_date

Процедура refresh_loan_holiday_info() загружает данные в витрину данных. 
Выполняется полная загрузка данных, так как: необходимо не только добавить новые строки, но и обновить существующие