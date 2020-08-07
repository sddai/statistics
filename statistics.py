import pandas as pd

'''
0801-0807，全部统计周期内，locationInfo出现频次Top10
单独每天内，locationInfo出现频次Top10
每天每个时段（0-1时、1-2时、2-3。。。。），locationInfo出现频次Top10
'''

'''
在此基础上做个统计：7天内重复出现频度最高的5个基站、当天内各时段重复出现频度最高的5个基站
'''

file1 = './bcm2M_he_202008(1).csv'
file2 = './bcm2M_gs_202008.csv'
df1 = pd.read_csv(file2)
# df2 = pd.read_csv(file2)
# print(df1.head(5))

df1 = df1.drop(labels=['mdn', 'sessionId', 'spid', 'duration', 'factDuration', 'resultCode', 'deleteType', 'location', 'time33MDN', 'ratType', 'imeiId', 'imsi', 'source'], axis=1)

df1['day'] = (round(df1['time'] // 1000000))
df1['hour'] = ((df1['time'] % 20200800000000) % 1000000 // 10000)

# print(df1['hour'])


def Top10(df1_Top10):
    df1_Top10 = pd.DataFrame(df1_Top10.loc[:,'locationInfo'].value_counts())
    df1_Top10.columns = ['freq']
    df1_Top10 = df1_Top10.sort_values(by=['freq'], ascending=False)
    df1_Top10 = df1_Top10.head(10)

    return df1_Top10


# 重复在前10中出现的，再取前五名
def Top5(df1_Top5):
    # df1_Top5 
    df1_Top5['original_location'] = df1_Top5.index
    df1_Top5 = pd.DataFrame(df1_Top5.loc[:,'original_location'].value_counts())
   
    df1_Top5.columns = ['duplicate_freq']
    df1_Top5 = df1_Top5.sort_values(by=['duplicate_freq'], ascending=False)
    df1_Top5 = df1_Top5.head(5)

    return df1_Top5


# print(df1.head(5))
df1_day1 = df1[df1['day'] == 20200801]
df1_day2 = df1[df1['day'] == 20200802]
df1_day3 = df1[df1['day'] == 20200803]
df1_day4 = df1[df1['day'] == 20200804]
df1_day5 = df1[df1['day'] == 20200805]
df1_day6 = df1[df1['day'] == 20200806]
df1_day7 = df1[df1['day'] == 20200807]

df1_Top10_whole = Top10(df1)
# df1_Top10_whole = df1_Top10_whole.sort(axis=0)
# df1_Top10_whole.drop_duplicates()
# df1_Top10_whole.to_csv('whole_Top10.csv', index=True)
df1_Top10_whole['day'] = 'whole'

df1_day1_top10 = Top10(df1_day1)
df1_day2_top10 = Top10(df1_day2)
df1_day3_top10 = Top10(df1_day3)
df1_day4_top10 = Top10(df1_day4)
df1_day5_top10 = Top10(df1_day5)
df1_day6_top10 = Top10(df1_day6)
df1_day7_top10 = Top10(df1_day7)

df1_day1_top10['day'] = 'day1'
df1_day2_top10['day'] = 'day2'
df1_day3_top10['day'] = 'day3'
df1_day4_top10['day'] = 'day4'
df1_day5_top10['day'] = 'day5'
df1_day6_top10['day'] = 'day6'
df1_day7_top10['day'] = 'day7'

""" df1_day1_top10.to_csv('day1_Top10.csv', index=True)
df1_day2_top10.to_csv('day2_Top10.csv', index=True)
df1_day3_top10.to_csv('day3_Top10.csv', index=True)
df1_day4_top10.to_csv('day4_Top10.csv', index=True)
df1_day5_top10.to_csv('day5_Top10.csv', index=True)
df1_day6_top10.to_csv('day6_Top10.csv', index=True)
df1_day7_top10.to_csv('day7_Top10.csv', index=True) """

# 合并七天
df1_day1to7_top10 = pd.concat([df1_Top10_whole, df1_day1_top10, df1_day2_top10, df1_day3_top10, df1_day4_top10, df1_day5_top10, df1_day6_top10, df1_day7_top10])

df1_day1to7_top10.to_csv('day1to7_top10.csv', index=True)
# print(df1_Top10_whole)


df_day1_all = None
df_day2_all = None
df_day3_all = None
df_day4_all = None
df_day5_all = None
df_day6_all = None
df_day7_all = None

# day1 24小时的循环：
for i in range(24):
    df1_hour_temp = df1_day1[df1_day1['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day1_hour%s_Top10.csv' % i, index=True)
    # df_day1_all.append(df1_hour_temp)
    df_day1_all = pd.concat([df_day1_all, df1_hour_temp])

    # print(df_day1_all)

    # df_day1_all['locationInfo'] = df_day1_all.index

# df_day1_all.to_csv('df_day1_hourly_top.csv')
for i in range(24):
    df1_hour_temp = df1_day2[df1_day2['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day2_hour%s_Top10.csv' % i, index=True)
    df_day2_all = pd.concat([df_day2_all, df1_hour_temp])


for i in range(24):
    df1_hour_temp = df1_day3[df1_day3['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day3_hour%s_Top10.csv' % i, index=True)
    df_day3_all = pd.concat([df_day3_all, df1_hour_temp])


for i in range(24):
    df1_hour_temp = df1_day4[df1_day4['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day4_hour%s_Top10.csv' % i, index=True)
    df_day4_all = pd.concat([df_day4_all, df1_hour_temp])


for i in range(24):
    df1_hour_temp = df1_day5[df1_day5['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day5_hour%s_Top10.csv' % i, index=True)
    df_day5_all = pd.concat([df_day5_all, df1_hour_temp])


for i in range(24):
    df1_hour_temp = df1_day6[df1_day6['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day6_hour%s_Top10.csv' % i, index=True)
    df_day6_all = pd.concat([df_day6_all, df1_hour_temp])


for i in range(24):
    df1_hour_temp = df1_day7[df1_day7['hour'] == i]
    df1_hour_temp = Top10(df1_hour_temp)
    df1_hour_temp.to_csv('day7_hour%s_Top10.csv' % i, index=True)
    df_day7_all = pd.concat([df_day7_all, df1_hour_temp])


'''在此基础上做个统计：7天内重复出现频度最高的5个基站、当天内各时段重复出现频度最高的5个基站'''
df1_day1to7_top10['locationInfo'] = df1_day1to7_top10.index
# print(df1_day1to7_top10.head(10))
df1_day1to7_duplicate5 = pd.DataFrame(df1_day1to7_top10.loc[:,'locationInfo'].value_counts())
df1_day1to7_duplicate5.columns = ['duplicate_in_top10_freq']
df1_day1to7_duplicate5 = df1_day1to7_duplicate5.sort_values(by=['duplicate_in_top10_freq'], ascending=False)
df1_day1to7_duplicate5 = df1_day1to7_duplicate5.head(5)

df1_day1to7_duplicate5.to_csv('day1to7_duplicate5_in_top10.csv')


'''当天内各时段重复出现频度最高的5个基站'''
df_day1_hourly_top5 = Top5(df_day1_all)
df_day1_hourly_top5['day'] = 'day1'
df_day1_hourly_top5.to_csv('day1_hourly_top5.csv')

df_day2_hourly_top5 = Top5(df_day2_all)
df_day2_hourly_top5['day'] = 'day2'
df_day2_hourly_top5.to_csv('day2_hourly_top5.csv')

df_day3_hourly_top5 = Top5(df_day3_all)
df_day3_hourly_top5['day'] = 'day3'
df_day3_hourly_top5.to_csv('day3_hourly_top5.csv')

df_day4_hourly_top5 = Top5(df_day4_all)
df_day4_hourly_top5['day'] = 'day4'
df_day4_hourly_top5.to_csv('day4_hourly_top5.csv')

df_day5_hourly_top5 = Top5(df_day5_all)
df_day5_hourly_top5['day'] = 'day5'
df_day5_hourly_top5.to_csv('day5_hourly_top5.csv')

df_day6_hourly_top5 = Top5(df_day6_all)
df_day6_hourly_top5['day'] = 'day6'
df_day6_hourly_top5.to_csv('day6_hourly_top5.csv')

df_day7_hourly_top5 = Top5(df_day7_all)
df_day7_hourly_top5['day'] = 'day7'
df_day7_hourly_top5.to_csv('day7_hourly_top5.csv')

# 合并七天
df1_day1to7_hourly_duplicate_top5 = pd.concat([df_day1_hourly_top5, df_day2_hourly_top5, df_day3_hourly_top5, df_day4_hourly_top5, df_day5_hourly_top5, df_day6_hourly_top5, df_day7_hourly_top5])
df1_day1to7_hourly_duplicate_top5.to_csv('【】df1_day1to7_hourly_duplicate_top5.csv', index=True)
