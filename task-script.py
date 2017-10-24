# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 12:44:52 2017

@author: TT
"""

"""
Написать скрипт, принимающий на вход detailed billing файлы:
    
https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-05.csv.zip
https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-06.csv.zip
https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-07.csv.zip
https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-2016-08.csv.zip
   
 Который посчитает суммарный cost по object_type, object_id для всех записей
    из файлов, для которых присутствует user:scalr-meta поле вида
v1:{SCALR_ENV_ID}:{SCALR_FARM_ID}:{SCALR_FARM_ROLE_ID}:{SCALR_SERVER_ID} 
 с непустыми значениями.
 
 Результат записать в sqlite таблицу:
     Field            Type
     object_type      Enum(env, farm, farm_role, server)  #working_dict[key][0]
     object_id        varchar(32) #key in working_dict
     cost             float #working_dict[key][1]

 
Где оbject_type - один из четырех видов ресурсов: env, farm, farm_role, server
А оbject_id - его значение в scalr-meta теге.
"""


#please set file pathways on your machine
file_path_0 = "D:/billing/615271354814-aws-billing-detailed-line-items-with-resources-and\
-tags-2016-05.csv"
file_path_1 = "D:/billing/615271354814-aws-billing-detailed-line-items-with-resources-and\
-tags-2016-06.csv"
file_path_2 = "D:/billing/615271354814-aws-billing-detailed-line-items-with-resources-and\
-tags-2016-07.csv"
file_path_3 = "D:/billing/615271354814-aws-billing-detailed-line-items-with-resources-and\
-tags-2016-08.csv"

file_pathes = [file_path_0, file_path_1, file_path_2, file_path_3]


db_name = 'taskDB1848'
class data_base_operations:
    """sqlite commands"""
    
        
    def data_base_creation():
        import sqlite3
        conn = sqlite3.connect(db_name, check_same_thread=False, timeout = 100) 
        c = conn.cursor()  
        create_table = '''
        CREATE TABLE IF NOT EXISTS Task_Table (object_id varchar(32),\
                                               object_type str, cost float)
            '''
        c.execute(create_table) 
        c.close()
        conn.close() 
        
    def count_cost():
        import sqlite3

        conn = sqlite3.connect(db_name) 
        c = conn.cursor()
        Sum = "SELECT SUM(Cost) FROM Task_Table"
        c.execute(Sum)
        Cost = c.fetchone()
        
        print(Cost)
        c.close()
        conn.close() 
        
        
        
    def print_table():
        import sqlite3
        conn = sqlite3.connect(db_name) 
        c = conn.cursor()
        query = "SELECT * FROM Task_Table;"
        c.execute(query)
        rows = c.fetchall()
        print ("Row data:")
        print  (len(rows))
        c.close()
        conn.close()

    def wipe_table():
        import sqlite3

        conn = sqlite3.connect(db_name) 
        c = conn.cursor()
        c.execute('DROP TABLE Task_Table')
        c.close()
        conn.close()  

    def execute_command(command):
        import sqlite3
        conn = sqlite3.connect(db_name, check_same_thread=False)\
                              #, timeout = 100 ) 
        c = conn.cursor()  
        c.execute(command)
        conn.commit()
        c.close()
        conn.close() 
         
def reader_csv(file_path):
    """reads csv files"""
    import csv
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]
    f.close()    
    return data


def empty_values_check(user_scalr_meta):
    return '' not in user_scalr_meta.split(':')
   

def data_processing(data):  
    """parses csv file, and returns a dictionary with task values"""
    working_dict = {}
    
    for record in range(len(data)):
        if empty_values_check(data[record]['user:scalr-meta']):
               obj_type = data[record]['user:scalr-meta'].split(':')
               if 'v1' in obj_type:
                   obj_type.remove('v1') 
               working_dict[data[record]['RecordId']] = [obj_type, data[record]['Cost']]
    print('DB lenghth: ', len(working_dict))           
    return working_dict         

def data_to_insert_generator(working_dict):
    """generates SQL commands"""
    return("INSERT INTO Task_Table (object_id, object_type, cost ) VALUES (%s, '%s', %s)" \
           % (''.join(k), ','.join(working_dict[k][0]), working_dict[k][1] ) \
             for k in working_dict)
    
    
def result_Table(working_dict):
   
    
    db = data_base_operations
    db.data_base_creation
    
    for command in data_to_insert_generator(working_dict):
        db.execute_command(command)


def files_processing(file_path):
    result_Table(data_processing(reader_csv(file_path)))




def task_execute():
    """multithreaded execution of whole task """
    import datetime    
    from multiprocessing.dummy import Pool as ThreadPool
    
    
    start_time = datetime.datetime.now()
    pool = ThreadPool()
    
    pool.map(files_processing, file_pathes)
    
    pool.close()
    pool.join()
    
    db = data_base_operations
   
    Cost = db.count_cost()
    
    print ('Time elapsed:', datetime.datetime.now() - start_time   )
    print ('Total Cost: ', Cost)
    #wipe_table()
    return Cost
