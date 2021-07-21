t1 = "tenantId=d36cd0cb-65a1-4550-87d9-3c125421e84b/y=2021/m=07/d=11/h=17/m=00/PT1H.json"
print(t1.split('/')[0])


t2 = "resourceId=/SUBSCRIPTIONS/E15EF64F-5D18-424A-B0F3-3FAF9D694D39/y=2021/m=07/d=20/h=14/m=00/PT1H.json"
print(t2.rsplit('/',1)[0])