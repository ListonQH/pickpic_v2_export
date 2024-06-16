import sqlite3
import os


class SqliteUtil():
    def __init__(self) -> None:
        self.__db_file_name = 'data.db'

        if not os.path.exists(self.__db_file_name):
            print(f'{self.__db_file_name} not find, create new!')

        self.__conn = sqlite3.connect(self.__db_file_name)
        self.__cur = self.__conn.cursor()

        self.__pickpic_v2_table_name = 'pickpic_v2_table'

        self.__table_head = ['id TEXT',
                             'source_file_name TEXT', 
                             'best_image_uid TEXT', 
                             'caption TEXT', 'caption_md5 TEXT',
                             'image_uid TEXT', 'image_md5 TEXT', 
                             'partner_img_uid TEXT','partner_img_md5 TEXT',
                             'self_better TEXT', 'model_better TEXT',
                             'nsfw_key_word TEXT NOT NULL DEFAULT "Y" ', 
                             'nsfw_2 TEXT  NOT NULL DEFAULT "Y" ', 
                             'nsfw_3 TEXT  NOT NULL DEFAULT "Y" ', 
                             'nsfw_4 TEXT  NOT NULL DEFAULT "Y" ', 
                             'nsfw_5 TEXT' ]

        if self.__find_target_table_name():
            self.__init_table()
        
        self.rows = 0
    
    def __find_target_table_name(self):
        sql = "select name from sqlite_master where type='table'"
        self.__cur.execute(sql)
        tables = self.__cur.fetchall()

        if len(tables) == 0:
            print('First time running ... ')
            return True
        else:            
            for t in tables:
                if self.__pickpic_v2_table_name in t:

                    sql = f"SELECT COUNT(*) FROM {self.__pickpic_v2_table_name}"
                    self.__cur.execute(sql)
                    self.rows = self.__cur.fetchone()[0]

                    print(f'Find table name: {self.__pickpic_v2_table_name}, rows: {self.rows} ...')
                    return False
            print(f'Not found target table: {self.__pickpic_v2_table_name} ... \n')
            return True

    
    def __init_table(self):
        sql = f"CREATE TABLE IF NOT EXISTS {self.__pickpic_v2_table_name} ( {', '.join(self.__table_head )} )"
        print(sql)
        self.__cur.execute(sql)
        self.__conn.commit()

    def insert(self, infos:dict):
        keys = [ k for k in infos.keys()]
        values = [ '\'' + infos[k] +'\'' for k in keys]
        
        # sql = f"INSERT INTO {self.__pickpic_v2_table_name} ( {', '.join(keys)} ) VALUES ( {', '.join(values)} )"
        # print(sql)
        # self.__cur.execute(sql)
        
        p = ['?'] * len(keys)
        sql = f"INSERT INTO {self.__pickpic_v2_table_name} ( {', '.join(keys)} ) VALUES ( {', '.join( p )} )"
        # print(sql)        
        self.__cur.execute(sql, values)
        self.__conn.commit()
        

if __name__=="__main__":
    h = SqliteUtil()
    infos = dict({'source_file_name':'123', 
             'best_image_uid':'asd', 
             'caption':'sss', 
             'caption_md5':'cx',
             'image_uid':'eer', 
             'image_md5':'fff', 
             'partner_img_uid':'ppo',
             'partner_img_md5':'ccvv'})
    
    infos['self_better'] = 'true' if infos['best_image_uid'] == infos['image_uid'] else 'false'
    
    h.insert(infos=infos)
