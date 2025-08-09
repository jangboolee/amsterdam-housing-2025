# amsterdam-housing-2025

Scrape, analyze, predict, and visualize the Amsterdam housing market with listed property data from [Pararius.nl](https://www.pararius.nl/).

## Prediction

Using scraped data, a `LightGBM` model was trained to predict asking prices using the following features:

* Size in square meters
* Number of rooms
* Building age
* Latitude
* Longitude
* Distance to Amsterdam Centraal station in kilometers

The asking price of properties was also log-transformed to reduce skew and stabilize variance across the properties. 
Due to limited training data, `k-fold` cross-validation was used, and the trained model ended up learning the following average feature importance across all folds:

<img width="1287" height="842" alt="feature_importance" src="https://github.com/user-attachments/assets/093f8011-da65-406f-b140-c40947030764" />

After 5 folds, the model's average Mean Absolute Error was $$82452.16$$ EUR, and the Root Mean Squared Error was $196166.33$ EUR. 

The trained model is not very accurate due to limited training data and features, but it is still possible to use the model as a baseline to detect properties with abnormal asking prices by seeing if the asking price falls within the range of `pred_price` $\pm MAE$:

<img width="600" height="800" alt="overvalued" src="https://github.com/user-attachments/assets/21cdc3a4-e4fa-40c3-92e8-447602450851" />

## Data visualization

### Median asking price/sqm per 4-digit postcode

* Static map
  
  <img width="5420" height="3570" alt="median_per_postcode4" src="https://github.com/user-attachments/assets/5d0c4f7a-ceb4-40f5-a0d3-5e10e9d75203" />

* Interactive map

  [Download link](https://github.com/jangboolee/amsterdam-housing-2025/blob/main/output/pc4_interactive_price_map.html): Download raw file, then open with a browser

### Median asking price/sqm per 6-digit postcode

* Static map

  <img width="5419" height="3575" alt="median_per_postcode6" src="https://github.com/user-attachments/assets/4271e3d6-31e1-4f03-94eb-87fd3692898c" />

* Interactive map

  [Download link](https://github.com/jangboolee/amsterdam-housing-2025/blob/main/output/pc6_interactive_price_map.html): Download raw file, then open with a browser

## Disclaimers

* The results use currently available property listings, so data is limited ($n=1911$).
  * Most 6-digit postcodes only contain a handful of listed properties, and many don't contain any listed properties at all, which makes grouping the data at the 4-digit postcode level more meaningful.
* For true property values, purchase price should be used instead of asking price, but purchase price is not publicly available.
* The scraping was only done on a city's property listing page (ex: [Amstedam koopwoningen listing page](https://www.pararius.nl/koopwoningen/amsterdam)), and not on individual property listing pages (ex: [property listing page for Zamenhofsraat 68](https://www.pararius.nl/huis-te-koop/amsterdam/ca317654/zamenhofstraat)). Therefore, only key attributes were retrieved per property, and many other attributes that can significantly impact a property's value (ex: erfpacht, energy label, VvE costs, terrace/garden availability, etc.) are not accounted for.

## DB schema

<img width="860" height="632" alt="amsterdam_scraping" src="https://github.com/user-attachments/assets/421cc1a9-7cd3-4583-b0ac-7340f711c85d" />

## Data sources

* Property listing data: [Pararius.nl](https://www.pararius.nl/)
* Amsterdam map data: [Gemeente Amsterdam: Maps Data](https://maps.amsterdam.nl/open_geodata/?LANG=en)
  *  `PC4_BUURTEN` dataset for 4-digit postcode polygons
  *  `PC6_PUNTEN_MRA` dataset for 6-digit postcode polygons and coordinates

## Possible next steps

* Add more features per property by scraping individual property pages, then train another model
* Scrape more properties outside of Amsterdam and expand the scope of analysis beyond Amsterdam
