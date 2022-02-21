import pandas as pd
from Base_DFs import raw_df
from Base_DFs import base_renamed_df

# -----------------METADATA----FUNCTIONS----------------------------------------------


def metadata_dict(renamed_df):
    # This function creates a dictionary holding the metadata (and the first row of the data.. should we delete this?)
    df = renamed_df.iloc[1:, :]
    df = df.loc[:(df == 'start').any(1).idxmax()]
    # This makes the dataframe into a dictionary where each row is a dictionary
    md_dict = df.to_dict(orient='records')
    return md_dict


def make_data_year_dict(md_dict):
    # This function creates a dictionary just holding the info for each column in the 'data year' row
    data_year_dict = {}
    for i in md_dict:
        for k, v in list(i.items()):
            if v == 'data year':
                data_year_dict = i
    # This deletes the key: value '0': 'data year'
    del data_year_dict[0]
    return data_year_dict


def make_universe_dict(md_dict):
    # This function creates a dictionary just holding the info for each column in the 'universe' row
    universe_dict = {}
    for i in md_dict:
        for k, v in list(i.items()):
            if v == 'universe':
                universe_dict = i
    del universe_dict[0]  # This deletes the key: value '0': 'universe'
    return universe_dict


def make_agg_method_dict(md_dict):
    # This function creates a dictionary just holding the info for each column in the 'aggregation method' row
    agg_method_dict = {}
    for i in md_dict:
        for k, v in list(i.items()):
            if v == 'aggregation method':
                agg_method_dict = i
    # This deletes the key: value '0': 'aggregation method'
    del agg_method_dict[0]
    return agg_method_dict


def make_column_dependent_dict(data_yr_dict, universe_dict, agg_method_dict):
    # Creating a dictionary of the three column dependent dictionaries
    col_dependent_dict = {}
    col_dependent_dict['data year'] = data_yr_dict
    col_dependent_dict['universe'] = universe_dict
    col_dependent_dict['aggregation method'] = agg_method_dict
    return col_dependent_dict


# -----------------------------------RUNNING----CODE-------------------------------------------------

# Calling the function to create a basic list of dictionaries of each row of the metadata
md_dict = metadata_dict(base_renamed_df)
print(md_dict)

# Pulling out the data year dictionary from the list of metadata dictionaries
d_y_dict = make_data_year_dict(md_dict)
print(d_y_dict)

# Pulling out the universe dictionary from the list of metadata dictionaries
u_dict = make_universe_dict(md_dict)

# Pulling out the aggregation method dictionary from the list of metadata dictionaries
# This will be used for aggregation method info later
agg_dict = make_agg_method_dict(md_dict)

# Making the column dependent dictionary
col_dependent_dict = make_column_dependent_dict(d_y_dict, u_dict, agg_dict)
print(col_dependent_dict)
