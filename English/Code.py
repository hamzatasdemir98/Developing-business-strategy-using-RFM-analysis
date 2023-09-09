
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', 1000)
 pd.set_option('display.max_rows', 40)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option("display.width",200)

## Task-1: Understanding and Preparing the data 

# Step-1: Read the dataset flo_data_20K.csv. Create copy of Dataframe.

df_ = pd.read_csv("flo_data_20k.csv")
df = df_.copy()
df.head()

# Step-2: Rewieving Dataset

# a. First 10 record,
df.head(10)
# b. Variable names,
df.columns
# c. Statistics,
df.describe().T
# d. Null values,
df.isnull().sum()
# e. Variable types
df.info()

# Step-3: Omnichannel means that customers shop both online and offline platforms. Create new variables for each customer's total number of purchases and spending

df["total_order"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
df["total_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

df.head()

# Step-4:Examine the variable types. Change the type of variables expressing date to date.

df[[col for col in df.columns if "date" in col]] = df[[col for col in df.columns if "date" in col]].apply(pd.to_datetime)
df.info()

# Step-5: See the distribution of the number of customers, total number of products purchased and total expenditures in shopping channels

df.groupby("order_channel").agg({"master_id" : "count",
                                 "total_order" : "sum",
                                 "total_value" : "sum"})

# Step-6: List the top 10 customers who bring the most profit

df.groupby("master_id").agg({"total_value" : "sum"}).sort_values(by='total_value', ascending=False).head(10)

# Step-7: List the top 10 customers who place the most orders

df.groupby("master_id").agg({"total_order" : "sum"}).sort_values(by="total_order", ascending=False).head(10)

# Step-8: Functionalize the data preparation process

def df_prep(dataframe):
    dataframe["total_order"] = dataframe["order_num_total_ever_offline"] + dataframe["order_num_total_ever_online"]
    dataframe["total_value"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]

    df[[col for col in df.columns if "date" in col]] = df[[col for col in df.columns if "date" in col]].apply(pd.to_datetime)

    return dataframe

## Task-2: Calculation of RFM Metrics

# Step-1: Describe Recency, Frequency, and Monetary values.

# Recency: The time that has passed since the customer's last purchase.
# Frequency:  Information on how many times the customer ordered
# Monetary: Total amount of orders placed by the customer

# Step-2: Calculate Recency, Frequency and Monetary metrics for the customer.

df["last_order_date"].max()
today_date = pd.to_datetime("2021-06-02")

#1. way
rfm = pd.DataFrame()
rfm["customer_id"] = df["master_id"]
rfm["recency"] = (today_date - df["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df["total_order"]
rfm["monetary"] = df["total_value"]

#2. way
rfm = df.groupby("master_id").agg({"last_order_date": lambda x: (today_date - x.max()).days,
                                     "total_order": lambda x: x.sum(),
                                     "total_value": lambda x: x.sum()})
rfm.columns = ["recency", "frequency", "monetary"]

## Task-3: Calculating RF Score

# Step-1: Convert Recency, Frequency and Monetary metrics into scores between 1-5 with the help of qcut.

rfm["recency_score"] = pd.qcut(rfm["recency"],5,labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels = [1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels= [1, 2, 3, 4, 5])

rfm.head(10)

rfm["RF_SCORE"] = rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str)

## Task-4: Defining RF Score as a Segment

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

## Task-5: Action time

# Step-1: Examine the recency, frequency and monetary averages of the segments.

rfm.groupby("segment").agg({"recency" : "mean",
                            "frequency" : "mean",
                            "monetary" : "mean"})

# Step-2: With the help of RFM analysis, find the customers in the relevant profile for the 2 cases given below and save the customer IDs as csv.

#a. FLO is adding a new women's shoe brand. The product prices of the included brand are above general customer preferences.
# For this reason, it is desired to specifically contact customers with the profile that 
# will be interested in the promotion of the brand and product sales. Customers who will be contacted specifically are
# champions, loyal_customers and people who shop in the female category. Save the ID numbers of these customers in the csv file.
rfm.reset_index(inplace=True)
rfm.head(15)

target_segment = rfm[rfm["segment"].isin(["champions", "loyal_customers"])]["master_id"]
df.head()
target = df[df["interested_in_categories_12"].str.contains("KADIN") & df["master_id"].isin(target_segment)]['master_id']
target.to_csv("kadın_loyal_champion_id.csv")


#b. Nearly 40% discount is planned for Men's and Children's products. It is intended to specifically target customers who
# are interested in the categories related to this discount, customers who have been good customers in the past
# but have not been shopping for a long time, sleepers and newly arrived customers.
# Save the IDs of customers with the appropriate profile in the csv file.

target_segment_2 = rfm[rfm["segment"].isin(["new_customers", "hibernating"])]["master_id"]

target_2 = rfm[rfm["master_id"].isin(target_segment_2) & (df["interested_in_categories_12"].str.contains("ERKEK") | df["interested_in_categories_12"].str.contains("COCUK"))]["master_id"]
target_2.to_csv("erkek_cocuk_hibernating_new_id.csv")
rfm[rfm["master_id"].isin(target_2)].head(30)


