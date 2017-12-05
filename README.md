# film-revenue-model
Uses a data set collected from IMDb and Box Office Mojo to create a model in R to predict the revenue of an upcoming film.

## Directions to Build Model
1. Install MongoDB on your machine and start it
2. Install Python2.7 and requirements.txt
3. Update GLOBALS.py if necessary
4. Run python src/run_data_collection.py in a terminal (this will take awhile)
5. Run python src/run_data_aggregation.py in a terminal
6. Open R code in src/model_generation
7. Run the relevant steps in the R Notebook to create the model

## Directions to Run Model
1. (Optional) Update the model coefficients in src/revenue_prediction/run_model.py
2. Run python src/predict_revenue.py {imdb link}

### Check [blog.jdnorthcott.com](http://blog.jdnorthcott.com/search/label/Final%20Project) for more details about the project process
