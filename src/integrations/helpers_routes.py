# [X] Pull data from transfers and create a pathways based on the amounts added to it.
# [ ] Pull sockets routes via API call
# [ ] Store the API calls status code for each pathway to better understand the data.

# from google.cloud import storage


# def get_upload_data_from_cs_bucket(greater_than_date, bucket_name, destination_table):
#     """
#     destination_table: BQ table where the data will be uplaoded
#     """
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket(bucket_name)
#     blobs = bucket.list_blobs()
#     for blob in blobs:
#         logging.info(f"Pulling data for: {blob.name}")

#         name = os.path.splitext(blob.name)[0]
#         dt = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")

#         if dt > greater_than_date:
#             data = json.loads(blob.download_as_text())
#             print(f"data: {len(data)}")

#             # convert the data to df
#             df = convert_json_to_df(json_file=data)
#             name = os.path.splitext(blob.name)[0]
#             df["upload_datetime"] = datetime.strptime(name, "%Y-%m-%d_%H-%M-%S")
#             df.columns = df.columns.str.lower()
#             df.columns = df.columns.str.replace(".", "_")
#             for col in df.columns:
#                 if df[col].apply(isinstance, args=(list,)).any():
#                     df[col] = df[col].apply(
#                         lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
#                     )

#                     df = df.astype(
#                         {col: "int" for col in df.select_dtypes(include=[bool]).columns}
#                     )

#             # upload to bq
#             pandas_gbq.to_gbq(
#                 dataframe=df,
#                 project_id=PROJECT_ID,
#                 destination_table=destination_table,
#                 if_exists="append",
#                 chunksize=100000,
#                 api_method="load_csv",
#             )

#         else:
#             logging.info(
#                 f"{dt} is not greater than {greater_than_date}, Data Already Added!"
#             )
