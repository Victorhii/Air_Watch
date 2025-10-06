

---

# 🌤️ AirWatch

### A Decision-Support Web App for Health-Focused School Activity Planning

**AirWatch** is a decision-support web application designed to help **school administrators** make health-focused choices about outdoor activities. Its core aim is simple: **reduce students’ exposure to harmful air** by providing clear, location-specific air quality forecasts and straightforward activity guidance (e.g., *Proceed / Proceed with Caution / Postpone*).

By translating technical pollutant forecasts into actionable recommendations tied to a school’s weekly schedule, AirWatch turns data into defensible decisions that prioritize student well-being.

---

## 🚀 Features

* 🌍 **Location-Specific Forecasts** – Uses satellite and ground data to generate school-level air quality predictions.
* 🧠 **AI-Powered Prediction** – XGBoost models trained on fused meteorological and pollutant data for 24–72 hour forecasts.
* 📊 **Interactive Dashboard** – View real-time pollutant levels, forecasts, and weekly activity planners.
* ⚠️ **Automated Recommendations** – Translates forecasted pollutant data into clear advisories for school activities.
* 📧 **Alert System (Optional)** – Sends notifications for critical air quality events.

---

## 🧩 Data Sources

AirWatch integrates multiple open and scientific data streams:

| Data Type     | Source                          | Description                                          |
| ------------- | ------------------------------- | ---------------------------------------------------- |
| O₃, HCHO, NO₂ | **NASA TEMPO**                  | Satellite-based trace gas retrievals                 |
| PM2.5         | **OpenAQ**                      | Ground-based sensor network                          |
| Meteorology   | **WeatherAPI** / **Open-Meteo** | Temperature, wind, humidity, and fallback predictors |

When TEMPO data are delayed, the system automatically leverages meteorological predictors and historical relations to maintain continuous forecasts.

---

## ⚙️ Technical Architecture

**Backend:** Python (Flask)
**Frontend:** HTML, CSS, JavaScript
**ML Framework:** XGBoost
**Data Handling:** pandas, xarray, geopandas, scikit-learn
**Spatial Tools:** pyproj, shapely

### System Flow

1. **Data Ingestion:** Satellite and sensor data are fetched and synchronized.
2. **Model Forecasting:** XGBoost predicts pollutant concentrations with uncertainty estimates.
3. **Decision Logic:** Recommendations are generated per school schedule.
4. **Frontend Display:** Flask serves interactive charts, maps, and advisories.

---

## 🧠 Model Overview

* Algorithm: **XGBoost Regressor**
* Forecast Horizon: **5 days**
* Features:

  * Pollutant trends and lag variables
  * Meteorological predictors (temperature, humidity, wind speed, boundary layer proxies)
  * Diurnal patterns
* Outputs:

  * Forecast concentrations (µg/m³ or ppbv)
  * Uncertainty ranges
  * Health-based AQI categories

Feature importance and SHAP summaries are used to maintain **explainability** and **trust** in automated decisions.

---

## 🧰 Installation & Setup

### Prerequisites

* Python 3.9+
* pip
* Git

### 1️⃣ Clone the repository

```bash
git clone https://github.com/yourusername/AirWatch.git
cd AirWatch
```

### 2️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 3️⃣ Run the Flask app

```bash
python app.py
```

### 4️⃣ Open in browser

Visit `http://127.0.0.1:5000`

---

## 🧑‍💼 Ethical & Operational Considerations

* No student data are collected; only school coordinates and admin contacts are stored.
* Data uncertainty and provenance are always displayed for transparency.
* Automated advisories **support**, not replace, human judgment.
* Calibration with local sensors is encouraged for better accuracy.

---

## 💡 Future Work

* Integration with live school calendar systems
* Low-cost sensor calibration for improved local accuracy
* Ensemble model improvements for extreme events (e.g., wildfires)
* Parent-facing mobile alert app

---

## 👥 Team & Contributions

Developed by a multidisciplinary team passionate about **air quality, education, and public health**.
Contributions are welcome! To contribute:

```bash
git checkout -b feature-branch
git commit -m "Add new feature"
git push origin feature-branch
```

Then open a pull request.

---



## 🌎 Acknowledgments

* NASA TEMPO Science Team
* OpenAQ community contributors
* WeatherAPI & Open-Meteo data providers
* XGBoost and scikit-learn open-source developers

---


