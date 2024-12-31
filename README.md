# Chicago Taxi System – Educational Project

This project was part of the Cubix Institute of Technology's 12+2 week online Data Engineer training. The curriculum revolved around this comprehensive project, enabling participants to learn about data sources, data structures, process design, and the steps of automated processing using Python's data science libraries. Subsequently, we migrated our system to Amazon AWS's data processing cloud services, providing insights into scalable, high-availability, and automation-enabled cloud-based data processing platforms.

![Chicago Taxi System](link-to-image)

## Project Goals and Details

As students, we assumed the role of Data Engineers at a large enterprise tasked with analyzing Chicago's taxi traffic, creating visualizations, and deriving business-supportive insights. While data analysts and managers defined business goals, our task was to collect, clean, and transform data into formats suitable for further statistical and scientific analysis. This required creativity and resourcefulness as data came from various sources with differing formats and cleanliness levels.

The primary data sources included:
- **City of Chicago Open Data API**: Provided foundational taxi data.
- **Wikipedia Thematic Pages**: Used for enriching data by linking community area codes and names through web scraping.
- **Open-Meteo API**: Integrated hourly weather data into the dataset.

The project was managed using Git and GitHub for version control and teamwork simulation.

## Results and Structure

1. **Local Data Pipeline**:
   Python scripts for accessing, collecting, and processing data locally.
   
2. **Cloud Adaptation**:
   AWS-adapted Python scripts, leveraging the `boto3` library for interacting with AWS services. These scripts are located in the `aws_pycode` folder.

3. **Visualizations**:
   At the end of the project, we created five visualizations showcasing different insights:
   - Anomalies in the data.
   - Revenue distribution among top taxi companies.
   - Payment type distribution.
   - Trip volume by hour.
   - Trip volume by day of the week.

### Dataset Overview
- **Period**: October 10, 2024 – November 1, 2024
- **Table Dimensions**: 240,953 rows x 35 columns (~8 million data points)
- **Anomalies Detected**: 30,727 records

### Visualizations
#### 1. **Anomalies by Taxi Companies**
   This chart highlights the types and sources of anomalies. It is notable that the highest anomaly rates did not always occur with the busiest companies.

   _Key Anomalies_:
   - **is_movement**: Inconsistent data about movement or distances traveled.
   - **is_payment**: Unclear payment statuses.
   - **is_real_trip**: Ambiguity regarding actual trips despite payments being recorded.

#### 2. **Revenue by Taxi Companies**
   Displays the top 10 companies by revenue during the examined period.

#### 3. **Payment Type Distribution**
   Highlights the distribution of payment types. Cash usage was unexpectedly high considering technological advancements in the USA.

#### 4. **Hourly Trip Distribution**
   Indicates peak taxi usage between 8 AM and 7 PM, which could inform shift scheduling and fleet allocation.

#### 5. **Daily Trip Distribution**
   A day-by-day analysis suggests that weekdays, particularly Thursdays and Fridays, are busier than weekends.

---

## Technical Implementation

This repository contains all scripts, including:
- Local Python scripts for processing data.
- AWS-adapted scripts for scalable cloud-based processing.

## Acknowledgments
Thanks to the Cubix Institute of Technology for providing this valuable learning opportunity.

---

For more details or to contribute, please feel free to open an issue or create a pull request.
