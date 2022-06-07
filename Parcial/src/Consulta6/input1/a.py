#%%

import pandas as pd


a = pd.read_csv("data.csv")

cols = a[ "Customer_Age" ,"Avg_Utilization_Ratio"]
cols.head()
# %%
a.columns()
# %%
a.head(

)
# %%
a.columns
# %%
cols = a[[ "Customer_Age" ,"Avg_Utilization_Ratio"]]

# %%
cols.head()
# %%
cols["Avg_Utilization_Ratio"] = cols["Avg_Utilization_Ratio"] * 100

# %%
cols.head()
# %%
cols["Avg_Utilization_Ratio"] = cols["Avg_Utilization_Ratio"].astype(int)
# %%
cols.head()

# %%
cols["Avg_Utilization_Ratio"] = cols["Avg_Utilization_Ratio"]//10


# %%
cols.head()

# %%
cols.groupby("Avg_Utilization_Ratio").mean()


# %%

cols["Customer_Age"] = cols["Customer_Age"]//10
cols.groupby("Customer_Age" ).count()
# %%

# %%

# %%
