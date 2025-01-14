# Report: Flight Delay Analysis Project

## Table of Contents
- [Report: Flight Delay Analysis Project](#report-flight-delay-analysis-project)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Project Goals](#project-goals)
  - [Pipeline](#pipeline)
    - [Data Sources](#data-sources)
    - [Ingestion Phase](#ingestion-phase)
    - [Staging Phase](#staging-phase)
      - [Data Cleansing](#data-cleansing)
      - [Data Transformations](#data-transformations)
      - [Enrichments](#enrichments)
    - [Production Phase](#production-phase)
      - [Analysis and Queries](#analysis-and-queries)
  - [Findings](#findings)
  - [Challenges Faced](#challenges-faced)
  - [Future Developments](#future-developments)

## Introduction
This project focuses on analyzing airline flight delays and their correlation with weather conditions at both departure and destination airports. Delays in air travel are a significant concern for airlines and passengers, as they affect operational efficiency and customer satisfaction. Understanding the factors contributing to delays, particularly weather, can provide valuable insights for improving airline operations and passenger experience.

The data used in this project includes detailed flight records and weather information such as wind speed, visibility, sky conditions, and temperature. By combining these datasets, we aim to uncover patterns and trends that highlight the impact of weather on flight delays.

## Project Goals
The project seeks to answer the following key questions:

Which airlines experience the most delays?
Identify the airlines with the highest frequency of delays to understand whether certain carriers face systemic issues.

Do bad weather conditions at the departure airport increase delays?
Examine whether factors such as high wind speeds, poor visibility, and adverse sky conditions contribute to delays when flights depart.

Do bad weather conditions at the destination airport increase delays?
Investigate how adverse weather conditions at arrival airports might correlate with delays.

## Pipeline

### Data Sources
The project utilizes two primary data sources:

Flight Data:
- Source: Aviation Edge API
- Scope: Direct flights between Orlando, Florida (MCO), and Palm Beach, Florida (FLL)
- Timeframe: January 1st to January 9th
- Details: Includes flight schedules, delays, and other operational data.

Weather Data:
- Source: Aviation Weather API
- Details: Weather conditions at both departure and destination airports, including wind speed, visibility, temperature, and sky conditions.

### Ingestion Phase
In the ingestion phase, data was fetched from various APIs and stored locally for further processing. The following steps were undertaken:

Flight Data Ingestion:
- The ingest_flight_data function was used to fetch flight history data for specified airports (MCO & FLL) from the Aviation Edge API.
- A rolling 10-day date range was used to retrieve flight records.
- The ingested data was stored in temporary JSON files.

Weather Data Ingestion:
- The ingest_weather_data function fetched weather data (e.g., wind, visibility, sky condition, temperature) from the Aviation Weather API.
- Weather data was collected for departure and arrival airports over the same date range as the flight data.
- The raw weather data was saved as JSON for further processing.

### Staging Phase

#### Data Cleansing before loading JSON to Postgres
Flight Data:
- Filtered flights to include only those departing from MCO or FLL and arriving at valid destinations.

#### Data Cleansing on Postgres relations
Weather Relation:
- Cleaned weather data to exclude rows with invalid formats:
  - Wind values ended with "KT".
  - Verified visibility values ended with "SM".
  - Filtered sky conditions to exclude improperly formatted entries.
  - Validated temperature and altimeter readings for numeric formats.

Flight Relation:
- Filtered flights for which no previous weather data was available in both departure and destination airports.

#### Data Transformations
Timestamp Standardization:
- Updated timestamps to conform to ISO 8601 standards, replacing lowercase "t" with uppercase "T" where necessary.

Association of Weather and Flights:
- Associated flights with the most recent weather records at both departure and arrival airports using PostgreSQL queries.
Updated flight records with departure_weather_id and destination_weather_id fields.

### Production Phase
In the production phase, cleaned and enriched data was exported for analysis and visualization.

CSV Export:
- The export_flights_with_weather function was used to generate a comprehensive CSV file containing flight details along with associated weather conditions.
- The exported file included columns for flight schedules, delays, weather metrics (e.g., wind, visibility), and airline identifiers.

Analysis Ready Dataset:
- The exported dataset served as the input for further analysis to answer the project's key questions, including:
- Identifying airlines with the most delays.
- Analyzing the correlation between weather conditions and delays.

#### Analysis and Queries
List the key analyses and queries performed to derive insights.  
--TODO--

## Findings
Summarize the key insights and findings from the analysis.  
--TODO--

## Challenges Faced
1. Data Accessibility: many APIs, such as the Aviation Edge API, require premium subscriptions for accessing historical data: we had to ask the saling team to get a free API key, which caused some delay in the project. Fortunately, the Aviation Weather API is free of charge.
2. Parsing METAR Weather Information: METAR weather data is highly technical and requires significant domain knowledge to interpret, especially because some METAR data were not including the same fields (wind direction, visibility, sky condition, and temperature). We had to use regular expressions to parse specific patterns in METAR strings, and apply filtering logic to discard incomplete or malformed data entries, such as weather rows with missing values or unexpected formats.
3. Correlation Analysis Challenges: The data set was relatively small, with only about 150 flights available for analysis over a 10-day period. This limited the statistical power of our correlation analysis, making it difficult to draw robust conclusions about the relationships between flight delays and weather conditions. Since the focus was on a specific route or airport pair (MCO to FLL), the lack of diversity in routes further constrained the generalizability of the findings. Moreover, it would be more than useful if the flight dataset included the cause of delay. It would enable us to answer the question: "Are the delayed flights, publicly announced as delayed because of the weather, actually delayed because of the weather?"

## Future Developments
1. Add More Routes: Expanding the analysis to include multiple routes and airports would provide a larger and more diverse dataset, allowing for more robust statistical analyses and insights. This would also help generalize findings to broader flight networks and not just specific pairs of airports.
2. Improved Data Sources: Partnering with additional API providers or acquiring premium subscriptions to services like Aviation Edge would allow access to richer historical and real-time flight and weather data. Integrating multiple data sources could also improve the accuracy of weather condition mapping and delay analysis.
3. Machine Learning for Delay Prediction: Leveraging machine learning models, such as regression or classification algorithms, would enable the analysis of complex weather conditions and their impact on flight delays. These models could also incorporate other variables (e.g., air traffic, airline performance) to predict delays with higher precision.
4. Real-Time Analysis: Developing a real-time analysis capability would allow airlines and passengers to receive up-to-date information about potential delays due to weather. This could be implemented using streaming data pipelines, real-time APIs, and dashboards for live monitoring.
5. Scaling the Pipeline: Scaling the data pipeline to handle larger datasets and more airports/routes would require optimization of data ingestion, storage, and processing. Using distributed systems, cloud-based storage, and parallel processing frameworks could help handle the increased load efficiently.