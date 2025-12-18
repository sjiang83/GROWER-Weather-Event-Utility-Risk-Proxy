# Extreme Weather & Utility Risk: An Event Study Baseline

This repository contains an end-to-end data pipeline and analysis exploring how extreme weather events act as physical shocks to the U.S. Utility Sector, using the **XLU ETF** as a financial proxy for grid stress. 

This project serves as a technical baseline for the **GROWER VIP team**, demonstrating the transition from administrative disaster data to actionable risk signals.

## 🛠 Project Structure
- `scripts/build_weather_events.py`: Python ETL script to pull and clean data from the FEMA OpenFEMA API.
- `data/weather_events.csv`: The processed event-level dataset (Aggregated by State & Event).
- `01_event_study_utility_risk.ipynb`: Core analysis including "Market Shock" aggregation and volatility response modeling.

## 🔍 Key Data Engineering Decisions
During the development, I encountered several "real-world" data challenges that required specific handling to ensure the statistical integrity of the Event Study:

### 1. Filtering Logic (Meteorological vs. Other Risks)
Although the raw Southeast dataset (GA, FL, SC, NC) contained various disaster types, I explicitly **excluded Fire events**. 
- **Reasoning:** In the Southeast, grid-impacting disasters are predominantly hydro-meteorological (Hurricanes, Tropical Storms). Fire declarations in this region often relate to smaller-scale management grants (FMAG) which lack the systemic "market-moving" impact of a major hurricane.

### 2. Handling "Administrative Clones"
FEMA data often records multiple entries for the same physical event in the same location. For example, in South Carolina (SC), Hurricane Ian appeared twice: an *Emergency Declaration* (EM) in September followed by a *Major Disaster Declaration* (DR) in November.
- **Solution:** I implemented a de-duplication layer to ensure each "onset" of a disaster is only counted once per region, preventing signal inflation.

### 3. The "Market Shock" Aggregation
A major hurricane (e.g., Hurricane Helene) often sweeps across multiple states, triggering separate FEMA disaster numbers for FL, GA, and NC. 
- **Strategy:** Financial markets are forward-looking and typically "price in" the shock upon the first landfall. To avoid diluting the volatility signal, I aggregated these state-level records into a single **Market Shock** based on a normalized event name and date.

### 4. Severity Proxies & Data Limitations
- **FIPS Coding Issues:** Initially, I attempted to use `fipsCountyCode` for high-resolution mapping, but found many records contained "0" or missing values in the API response. I switched to a robust count of unique `designatedArea` to maintain consistency.
- **Dynamic Filtering:** Instead of hard-filtering small events during ETL, I kept the full dataset and applied a threshold (e.g., `affected_areas_count > 5`) within the Notebook to allow for sensitivity testing without losing raw information.

## 📈 Analysis Methodology
By constructing a **Composite Risk Score** (standardizing 30-day rolling volatility and maximum drawdown), the analysis reveals a visible "Risk Spike" in the XLU sector following the onset (T=0) of major weather shocks. 



## 🚀 Future Work (Connection to GROWER)
- **From Financial Proxies to Physical Truth:** Replace XLU market data with real-time **Outage Data** (duration/frequency) to build a direct resilience model.
- **Spatial Precision:** Moving from state-level aggregation to high-resolution GIS analysis, addressing the fact that a high county count does not always correlate with the total impacted geographic area.
- **Resource Optimization:** Use the Risk Score framework to develop predictive "Readiness Triggers" for emergency response and resource pre-positioning.
