#!/bin/bash

# Assuming the compiled JS file is in the same directory as the TS file


# Replace 'your-bucket-name' and 'path/to/assets.json' with your actual bucket name and desired path
gsutil cp assets.json gs://your-bucket-name/path/to/assets.json