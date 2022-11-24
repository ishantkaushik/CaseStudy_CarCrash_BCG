This is case study of CarCrash 

Run the below code in terminal to execute and run the code

unzip data.zip


zip -r src.zip transform.py utils.py loadfiles.py


spark-submit --master local --py-files src.zip --files config.yaml app.py
