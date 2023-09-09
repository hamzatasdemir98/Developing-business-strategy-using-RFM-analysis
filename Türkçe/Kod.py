
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', 1000)
 pd.set_option('display.max_rows', 40)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option("display.width",200)

## Görev 1: Veriyi Anlama ve Hazırlama

# Adım 1: flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.
df_ = pd.read_csv("datasets/flo_data_20k.csv")
df = df_.copy()
df.head()

# Adım 2: Veri setinde
# a. İlk 10 gözlem,
df.head(10)
# b. Değişken isimleri,
df.columns
# c. Betimsel istatistik,
df.describe().T
# d. Boş değer,
df.isnull().sum()
# e. Değişken tipleri, incelemesi yapınız
df.info()

# Adım 3: Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Her bir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz

df["total_order"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
df["total_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

df.head()

# Adım 4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.

df[[col for col in df.columns if "date" in col]] = df[[col for col in df.columns if "date" in col]].apply(pd.to_datetime)
df.info()

# Adım 5: Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız

df.groupby("order_channel").agg({"master_id" : "count",
                                 "total_order" : "sum",
                                 "total_value" : "sum"})

# Adım 6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız

df.groupby("master_id").agg({"total_value" : "sum"}).sort_values(by='total_value', ascending=False).head(10)

# Adım 7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız

df.groupby("master_id").agg({"total_order" : "sum"}).sort_values(by="total_order", ascending=False).head(10)

# Adım 8: Veri ön hazırlık sürecini fonksiyonlaştırınız

def df_prep(dataframe):
    dataframe["total_order"] = dataframe["order_num_total_ever_offline"] + dataframe["order_num_total_ever_online"]
    dataframe["total_value"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]

    df[[col for col in df.columns if "date" in col]] = df[[col for col in df.columns if "date" in col]].apply(pd.to_datetime)

    return dataframe

## Görev 2: RFM Metriklerinin Hesaplanması

# Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.

# Recency: Müşterinin son alışveriş yaptığı tarihten itibaren geçen süre.
# Frequency: Müşterinin kaç defa sipariş verdiği bilgisi
# Monetary: Müşterinin verdiği siparişlerin toplam tutarı

# Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.

df["last_order_date"].max()
today_date = pd.to_datetime("2021-06-02")

#1. yol
rfm = pd.DataFrame()
rfm["customer_id"] = df["master_id"]
rfm["recency"] = (today_date - df["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df["total_order"]
rfm["monetary"] = df["total_value"]

#2. yol
rfm = df.groupby("master_id").agg({"last_order_date": lambda x: (today_date - x.max()).days,
                                     "total_order": lambda x: x.sum(),
                                     "total_value": lambda x: x.sum()})
rfm.columns = ["recency", "frequency", "monetary"]

## Görev 3: RF Skorunun Hesaplanması

# Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.

rfm["recency_score"] = pd.qcut(rfm["recency"],5,labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels = [1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels= [1, 2, 3, 4, 5])

rfm.head(10)

rfm["RF_SCORE"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

## Görev 4: RF Skorunun Segment Olarak Tanımlanması

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm["segment"] = rfm['RF_SCORE'].replace(seg_map, regex=True)

## Görev 5: Aksiyon Zamanı

# Adım 1: Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm.groupby("segment").agg({"recency" : "mean",
                            "frequency" : "mean",
                            "monetary" : "mean"})

#Adım 2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.

#a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
#tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak
#iletişime geçmek isteniliyor. Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş
#yapan kişiler özel olarak iletişim kurulacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına kaydediniz.
rfm.reset_index(inplace=True)
rfm.head(15)

target_segment = rfm[rfm["segment"].isin(["champions", "loyal_customers"])]["master_id"]
df.head()
target = df[df["interested_in_categories_12"].str.contains("KADIN") & df["master_id"].isin(target_segment)]['master_id']
target.to_csv("kadın_loyal_champion_id.csv")


#b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte
#iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni
#gelen müşteriler özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz

target_segment_2 = rfm[rfm["segment"].isin(["new_customers", "hibernating"])]["master_id"]

target_2 = rfm[rfm["master_id"].isin(target_segment_2) & (df["interested_in_categories_12"].str.contains("ERKEK") | df["interested_in_categories_12"].str.contains("COCUK"))]["master_id"]
target_2.to_csv("erkek_cocuk_hibernating_new_id.csv")
rfm[rfm["master_id"].isin(target_2)].head(30)


