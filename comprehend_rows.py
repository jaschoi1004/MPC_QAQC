import pandas as pd

from Base_DFs import base_renamed_df
from Base_DFs import raw_df

# THE FOLLOWING CODE FINDS THE ROWS HOLDING SUM VALUES (TOTAL ROWS)


def make_list_of_cols(renamed_df):
    # This function takes in the dataframe with headers from 0-N
    # and creates a list of all of the column headers in that csv
    # (basically a list of numbers from 0-N)
    list_of_col_headers = list(renamed_df)
    return list_of_col_headers


def find_n_cols_to_delete(raw_df):
    # This function takes in the RAW Dataframe (not the dataframe with headers from 0-N)
    # and is used to find the columns that should be deleted when creating the
    # dictionary of total rows and rows to be summed
    data_frame = raw_df.fillna('none')
    data_frame.set_index('x', inplace=True)
    data_frame = data_frame.drop(['start', 'none', 'end'])
    data_frame = data_frame.reset_index()
    data_frame = data_frame.loc[:, :'start']
    data_frame = data_frame.drop(['start'], axis=1)
    n_cols_to_delete = len(data_frame.columns)
    return n_cols_to_delete


def clean_data_frame_part_1(df, number_of_cols, col_header_list):
    # This function takes in the dataframe with headers 0-N,
    # the list of columns to be deleted and the list holding all of the headers
    # This function creates a dataframe without the metadata
    # columns except for the last column of metadata (see line 47)
    # so that column can be used to find the 'none' values
    N = number_of_cols
    del col_header_list[0:N-1]
    clean_col_df = df[col_header_list]
    return clean_col_df


def find_n_rows_to_delete(raw_df):
    # This function takes in the RAW Dataframe
    # and returns the length of the metadata
    # aka number of rows of metadata
    data_frame = raw_df.fillna('none')
    data_frame.set_index('x', inplace=True)
    data_frame = data_frame.drop(['start', 'none', 'end'])
    data_frame = data_frame.reset_index()
    metadata_dict = data_frame.set_index('x').T.to_dict()
    metadata_length = len(metadata_dict.keys())
    return metadata_length


def clean_data_frame_part_2(df, length_value):
    # This function takes in the cleaned dataframe (from which the columns of metadata were deleted)
    # and the number of metadata rows
    # and returns a dataframe without any rows of metadata
    N = length_value
    clean_clean_df = df.iloc[N+1:, :]
    return clean_clean_df


def find_first_col(clean_clean_df):
    # This function finds what the first column header is within the doubly cleaned dataframe
    list_of_col_headers = list(clean_clean_df)
    first_col_index = list_of_col_headers[0]
    return first_col_index


def find_indices_of_total_rows(cleaned_df, first_col_index):
    # This function takes in the doubly cleaned dataframe and the header of the first column in that dataframe
    # This function returns the index of the rows that hold totals
    # These indices are in a list
    cleaned_df_filled = cleaned_df.fillna('none')
    # This is a dataframe
    total_rows = cleaned_df_filled.loc[cleaned_df_filled[first_col_index] == 'none']
    total_rows_dict = total_rows.to_dict('split')
    total_rows_index = total_rows_dict['index']
    return total_rows_index


def find_rows_to_sum(cleaned_df, first_col_index):
    # This function takes in the doubly cleaned dataframe and the header of the first column in that dataframe
    # This function returns the index of the rows that should be aggregated to get the value in the total row
    # These indices are in a list
    cleaned_df_filled = cleaned_df.fillna('none')
    # This is a dataframe
    rows_to_sum = cleaned_df_filled.loc[cleaned_df_filled[first_col_index] != 'none']
    rows_to_sum_dict = rows_to_sum.to_dict('split')
    rows_to_sum_index = rows_to_sum_dict['index']
    return rows_to_sum_index


def make_row_dict(rows_to_sum_index, total_rows_index):
    # This function takes in the two lists - the list of the indices of rows to be aggregated
    # and the list of the indices of rows holding the totals
    # This function creates a dictionary of dictionaries
    row_dict = {}
    row_dict['rows to sum'] = rows_to_sum_index
    row_dict['total rows'] = total_rows_index
    return row_dict


# ---------------------------------------

# Making a list of all of the columns in the base dataframe
col_header_list = make_list_of_cols(base_renamed_df)

# Finding the columns to delete to create a dataframe with no columns of metadata
cols_to_delete = find_n_cols_to_delete(raw_df)

# Cleaning the dataframe by deleting the columns as found above
clean_df_col = clean_data_frame_part_1(
    base_renamed_df, cols_to_delete, col_header_list)

# Finding the length of the metadata (number of rows at the top)
md_len = find_n_rows_to_delete(raw_df)

# Cleaning the dataframe by using this function to delete all rows of metadata
clean_clean_df = clean_data_frame_part_2(clean_df_col, md_len)

# Finding the index of the column that starts the cleaned dataframe after cleaning
first_col_index = find_first_col(clean_clean_df)

# Finding the 'TOTAL' rows within the cleaned dataframe
total_rows = find_indices_of_total_rows(clean_clean_df, first_col_index)

# Finding the rows that will be aggregated within the cleaned dataframe
rows_to_sum = find_rows_to_sum(clean_clean_df, first_col_index)

# Making the dictionary to hold the rows that will be aggregated and the rows that hold the totals
row_dict = make_row_dict(rows_to_sum, total_rows)
