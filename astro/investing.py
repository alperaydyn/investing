import os
import time
import datetime as dt
import pandas as pd
import investpy
import sqlite3
import ta
import sys
import argparse

class investing():
    db_path = "symbols.db"
    cnn = None
    cursor = None

    def __init__(self):
        print('başlıyor...')
        # veritabanına bağlan
        # var mı kontrol et
        db_var = os.path.exists(self.db_path)
        self.cnn = sqlite3.connect(self.db_path)
        self.cursor = self.cnn.cursor()

        # eğer db yoksa tabloları oluştur
        if not db_var:
            self.cursor.execute("""
                create table search_list (id integer primary key autoincrement, type text, country text, symbol text, name text, groupname text)
            """).connection.commit()

            self.cursor.execute("""
                create table historic (list_id integer, date text, open float, high float, low float, close float, volume float, currency text)
            """).connection.commit()

            print("veritabanı oluşturuldu.")

        #for row in self.cursor.execute('select * from historic limit 10').fetchall():
        #    print(row)
        self.cursor.execute("""delete from historic where list_id>=22 and date>'2020-08-01'""")
        self.cnn.commit()

    def search(self):
        q = input("aranacak metni girin (min 3 karakter):\n")
        if len(q)<3:
            # boş veya kısa metin girilmişse False döndür, böylece tekrar seçim soracak
            print("geçersiz giriş, min 3 karakter")
            return False

        else:
            # arama işlemini gerçekleştir
            sr = investpy.search.search_quotes(q)

            # sonuçları pair_type'a göre sırala
            sr = sorted([[s.pair_type, s.country, s.symbol, s.name] for s in sr], key=lambda x:x[0])

            # arama sonuçlarını göster, başına seçim için (i) index koy
            for i,s in enumerate(sr):
                print(i+1,'\t', 
                     s[0].ljust(20,' '), 
                     s[1].ljust(15,' '),
                     s[2].ljust(20,' '),
                     s[3])

            # seçim numarasını al
            j = input("Seçiminizi giriniz: ")

            # girilen değer sonuçlar içerisinde değilse False döndür, böylece tekrar seçim soracaktır
            if not j in map(str,range(1,i+2)):
                print("Hatalı seçim")
                return False
            
            # seçilen smbolü göster
            self.selected = sr[int(j)-1]
            print("seçilen", ''.join([f.ljust(20,' ') for f in self.selected]))

            # kaydetme onayı al
            sc = input("kaydedilsin mi? (E/H)\n")
            if sc=="E" or sc=="e":
                group_name = input("grup adı:\n")
                self.selected = self.selected + [group_name]

                sql = """
                    INSERT INTO search_list (type, country , symbol, name, groupname) values (?,?,?,?,?)
                """
                self.cursor.execute(sql, self.selected).connection.commit()
                si = self.cursor.lastrowid
                print("seçim kaydedildi: ", si)

                self.display_searchlist()

                # verileri güncelle
                vg = input("veriler güncellensin mi? (E/H)\n")
                if vg=="E" or vg=="e":
                    self.retrieve_single_symbol(list_id=si, pair_type=self.selected[0], country=self.selected[1], symbol=self.selected[2], name=self.selected[3])
                    s = self.display_searchlist()

        return True

    def display_searchlist(self, display=True):
        s = self.cursor.execute("""
            select 
                t1.id, t1.type, t1.country, t1.symbol, t1.name, t1.groupname,
                count(t2.list_id) as adet, min(t2.date) as min_tarih, max(t2.date) as max_tarih 
            from search_list t1 
            left join historic t2 on t1.id=t2.list_id
            group by
                t1.id, t1.type, t1.country, t1.symbol, t1.name
        """).fetchall()
        if len(s)==0:
            print("sembol bulunamadı")
        elif display:
            for row in s: 
                print(''.join([str(f)[:14].ljust(15, ' ') for f in row]))
        return s

    def delete_symbol(self):
        s = self.display_searchlist()

        di = input("silinecek sembol id'sini girin:\n")
        if di in (str(r[0]) for r in s):
            self.cursor.execute("delete from search_list where id=?", (int(di),))
            self.cursor.execute("delete from historic where list_id=?", (int(di),))
            self.cnn.commit()
        else:
            print("bulunamadı")

    def read_list(self):
        s = self.display_searchlist(display=False)
        for row in s:
            #print("başladı: ", row[3])
            t1 = time.time()
            self.retrieve_single_symbol(list_id=row[0], pair_type=row[1], country=row[2], symbol=row[3], name=row[4])
            t2 = time.time()
            #print("tamamlandı: ", row[3], round(t2-t1, 2), 'sn.')

        s = self.display_searchlist(display=False)
        return s

    def retrieve_single_symbol(self, list_id, pair_type, country, symbol, name):
        # sembolü historyde ara
        sh = self.cursor.execute("select max(date) from historic where list_id=?", (list_id, )).fetchall()
        #print(list_id, sh)

        if sh[0][0]==None:
            # yoksa tarih aralığını tüm data olarak seç
            from_date_s = '01/01/1900'
            from_date_d = dt.datetime.strptime(from_date_s,'%d/%m/%Y')
            to_date_d = dt.datetime.now()+dt.timedelta(days=1)
            to_date_s = to_date_d.strftime("%d/%m/%Y")
        else:
            # varsa en son tarihi bul, bir artır
            from_date_d = (dt.datetime.strptime(sh[0][0],'%Y-%m-%d %H:%M:%S')+dt.timedelta(days=1))
            from_date_s = from_date_d.strftime('%d/%m/%Y')
            to_date_d = dt.datetime.now()+dt.timedelta(days=1)
            to_date_s = to_date_d.strftime("%d/%m/%Y")

        print(pair_type.ljust(15,' '), 
              symbol.ljust(20,' '), 
              from_date_s, to_date_s, '\t', end='\t')
        if from_date_d.strftime("%Y-%m-%d") < to_date_d.strftime("%Y-%m-%d"):
            # sembolü çek
            try:
                if pair_type=="stocks":
                    df = investpy.get_stock_historical_data(stock=symbol, country=country, from_date=from_date_s, to_date=to_date_s)
                elif pair_type=="indices":
                    df = investpy.get_index_historical_data(index=name, country=country, from_date=from_date_s, to_date=to_date_s)
                elif pair_type=="commodities":
                    df = investpy.get_commodity_historical_data(commodity=symbol, country=country, from_date=from_date_s, to_date=to_date_s)

                # veritabanına kaydet
                df.reset_index(inplace=True)
                df.columns = ['date','open','high','low','close','volume','currency']
                df['list_id'] = list_id
                df.to_sql('historic',self.cnn, if_exists='append', index=False)
                print("kaydedildi,", len(df), ' adet')

            except Exception as ex:
                print('HATA:', ex)
        else:
            print('en güncel data mevcut')

        return 

    def calculate_indicators(self, list_id):
        df = pd.read_sql('select t1.*, t2.symbol from historic t1 left join search_list t2 on t1.list_id=t2.id where list_id=%s'%list_id, self.cnn)
        dfi = df.loc[:,['list_id','symbol','date','close']]
        # RSI
        dfi['RSI_14'] = ta.momentum.rsi(df['close'],n=14)
        
        # Stochastic
        temp = ta.momentum.StochasticOscillator(high=df['high'], low=df['low'], close=df['close'], n=9, d_n=6)
        dfi['STOCH_9_6'] = temp.stoch()
        dfi['STOCH_9_6_S'] = temp.stoch_signal()

        # MACD
        temp = ta.trend.MACD(close=df['close'],n_slow=26, n_fast=12, n_sign=9)
        dfi['MACDD2'] = temp.macd_diff()
        dfi['MACD_2'] = temp.macd()
        dfi['MACDS2'] = temp.macd_signal()

        # ADX
        temp = ta.trend.ADXIndicator(high=df['high'], low=df['low'], close=df['close'], n=14)
        dfi['ADX'] = temp.adx().apply(lambda x: 1 if x>25 else -1 if x<20 else 0)

        # WR
        temp = ta.momentum.WilliamsRIndicator(high=df['high'], low=df['low'], close=df['close'], lbp=14)
        dfi['WR'] = temp.wr().apply(lambda x: 1 if x>-20 else -1 if x<-80 else 0)

        # Awesome 
        temp = ta.momentum.AwesomeOscillatorIndicator(high=df['high'], low=df['low'], s= 5, len= 34, fillna= False)
        dfi['AI'] = temp.ao()

        print(dfi.tail(15))

    def export_symbol(self, list_id):
        s = self.display_searchlist()
        ei = input("Kaydedilecek sembol id'sini girin:\n")
        file_path = os.get_wd()
        print('kaydedildi, ', file_path)

def execute_selection(ui):
    # r döngüye devam etme göstergesi
    r = True

    if ui=="0": # komut satırından parametre girilmeden çağırılması durumunda
        pass

    elif ui=="1": #arama yap, arama bitince tekrar seçim sor
        r = ci.search()

    elif ui=="2": #sembolleri göster,  tekrar seçim sor
        r = ci.display_searchlist()
 
    elif ui=="3": # arama listesinden sembol sil, sildikten sonra tekrar seçim sor
        ci.delete_symbol()

    elif ui=="4": # arama listesini çalıştır, bitince çıkış yap, uzun aramalar sonrasında otomatik kapama için
        ci.read_list()
        r = False

    elif ui=="5": # sembol çıktısı al
        ci.export_symbol()
        
    elif ui=="q" or ui=="Q": # çıkış
        r = False

    else: # hatalı girişte tekrar seçim sor
        print("Geçersiz seçim:", ui)
        r = True
    
    return r

if __name__=="__main__":
    ci = investing()
    #ci.calculate_indicators(3)
    
    parser = argparse.ArgumentParser()
    parser.add_argument( '-m','--mode', default="0", help='çalışma modunu seçiniz: {1:sembol ekle, 2:sembol göster, 3:sombol sil, 4:verileri güncelle, 5:çıktı al}')
    args = parser.parse_args()

    if args.mode=="0":
        while True:
            ui = input("\n" + \
                "Seçiminizi girin\n" + \
                "1: sembol ekle\n" + \
                "2: sembol göster\n" + \
                "3: sembol sil\n" + \
                "4: verileri güncelle\n" + \
                "5: çıktı al\n" + \
                "q: çıkış\n"
            )
            r = execute_selection(ui)
            if not r: break
    else:
        execute_selection(args.mode)

    
    ci.cursor.close()
    ci.cnn.close()
    print('db closed')
