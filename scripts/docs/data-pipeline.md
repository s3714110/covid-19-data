# Data pipeline
To produce [our dataset](../dataset) we are constantly developing our dedicated library [cowidev](../cowidev/index). This library provides us with the
command tool [`cowid`](../cowidev/cowid-api) which eases:

1. Running several _sub-processes_ (or pipelines) that generate _intermediate datasets_.
2. Jointly processing and merging all these intermediate datasets into the final and complete dataset.  

Consequently, the dataset is updated multiple times a day (_at least_ at 06:00 and 18:00 UTC), using the latest generated intermediate datasets.


## Overview
The dataset pipeline is built from several pipelines, which are executed independently and whose outputs are combined in
a final step. The complexity of the pipelines varies. For instance, for vaccinations, testing and hospitalization
we are responsible for collecting, processing and publishing the data but for cases/deaths we leave the collection step to [Johns
Hopkins Coronavirus Resource Center](https://coronavirus.jhu.edu/map.html) and then transform and publish the data.

The table below lists all the constituent pipelines, along with their execution frequencies, and what are the pipelines'
tasks.

| **Pipeline**              | **Frequency**                | **Tasks**                             |
|---------------------------|------------------------------|------------------------------------------|
| [Vaccinations](#vaccinations-pipeline)               | every weekday at 12:00 UTC           | {abbr}`Collection (Scraping primary sources (e.g. country governmental sites) and extracting relevant datapoints.)`, {abbr}`transformation (Transforming and cleaning the downloaded data into a human-readable format.)`, {abbr}`presentation (Presenting the cleaned data to the public (e.g. charts, dataset files, etc.).)` |
| [Testing](#testing-pipeline)                   | 3 times per week             | Collection, transformation, presentation |
| [Hospitalization & ICU](#hospitalization-icu-pipeline)     | daily at 06:00 and 18:00 UTC | Collection, transformation, presentation |
| [Cases & Deaths (JHU)](#cases-deaths-jhu-pipeline)      | daily at 04:00, 10:00, 16:00 and 22:00 UTC     | Transformation, presentation             |
| [Excess mortality](#excess-mortality-pipeline)          | weekly | Transformation, presentation             |
| [Variants](#variants-pipeline)                  | daily at 20:00 UTC           | Transformation, presentation             |
| [Reproduction rate](#reproduction-rate-pipeline)         | daily                        | Presentation                             |
| [Policy responses (OxCGRT)](#policy-responses-oxcgrt-pipeline) | daily                        | Transformation, presentation             |
| [Public monitor (YouGov)](#public-monitor-yougov-pipeline) | weekly                        | Transformation, presentation             |

You can find all the automation details [in this file](https://github.com/owid/covid-19-data/blob/master/scripts/scripts/autoupdate.sh).

## Vaccinations pipeline
The vaccination pipeline is probably the most complete one, where we scrape and extract data for each country in the
dataset.

The pipeline is executed manually, by [@edomt](https://github.com/edomt) or [@lucasrodes](https://github.com/lucasrodes)
every weekday (i.e. Monday until Friday) before 12 UTC.

### Execution steps
```
# Download/scrape data
cowid vax get

# Proces/check data
cowid vax process

# Generate dataset
cowid vax generate

# Integrate into full dataset
cowid vax export
```

```{seealso}

[Intermediate dataset](https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/), including per-country files and data technical details.
```

## Testing pipeline
We scrape and process data for multiple countries, similarly to the vaccinations pipeline. The pipeline is executed manually, by [@camapel](https://github.com/camapel) on Mondays and Fridays.

:::{warning}
The testing pipeline is [under refactoring](https://github.com/owid/covid-19-data/discussions/2099).
:::

### Execution steps

```
# Download/scrape data
cowid testing get
```

```{seealso}
[Intermediate datasets](https://github.com/owid/covid-19-data/tree/master/public/data/testing)
```
## Hospitalization & ICU pipeline
We scrape and process the data similarly as to what we do for testing and vaccinations. The pipeline is run daily.

### Execution steps

```
# Download data & generate dataset
cowid hosp generate

# Update Grapher-ready files
cowid hosp grapher-io

# Update Grapher database
cowid hosp grapher-db
```

```{seealso}

[Intermediate dataset and data technical details](https://github.com/owid/covid-19-data/tree/master/public/data/hospitalizations).
```

## Cases & Deaths (JHU) pipeline
We source cases and death figures from the [COVID-19 Data Repository by the Center for Systems Science and Engineering
(CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19). We transform some of the variables and
re-publish the dataset.
### Execution steps

```
# Download data
cowid jhu download

# Generate dataset
cowid jhu generate

# Update Grapher database
cowid jhu grapher-db
```


```{seealso}

[Intermediate dataset](https://github.com/owid/covid-19-data/tree/master/public/data/jhu).
```

## Excess Mortality pipeline
The pipeline is manually executed once a week. The reported all-cause mortality data is from the [Human Mortality Database](https://www.mortality.org/) (HMD) Short-term Mortality Fluctuations project and the [World Mortality Dataset](https://github.com/akarlinsky/world_mortality) (WMD). Both sources are updated weekly. We also present estimates of excess deaths globally that are [published by _The Economist_](https://github.com/TheEconomist/covid-19-the-economist-global-excess-deaths-model).


### Execution steps

```
# Download data and generate dataset
cowid xm generate
```

```{seealso}

[Intermediate dataset and data technical details](https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality).
```

## Variants pipeline
We run this pipeline daily. 
### Execution steps

```
# Download data and generate dataset
cowid variants generate

# Update Grapher-ready files
cowid variants grapher-io
```

```{note}
The data on variants and sequencing is indeed no longer available to download.
It is published by GISAID under a license that doesn't allow us to redistribute it.
Please visit [the data publisher's website](https://www.gisaid.org/) for more details. You may want to register an account there if you're really interested in using this data.
```
## Reproduction rate pipeline
We source the data from [crondonm/TrackingR/](https://github.com/crondonm/TrackingR/).

```{seealso}
[_Tracking R of COVID-19 A New Real-Time Estimation Using the Kalman Filter_](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0244474), by Francisco Arroyo, Francisco Bullano, Simas Kucinskas, and Carlos Rondón-Moreno

```
## Policy responses (OxCGRT) pipeline

:::{warning}
The OxCGRT pipeline is under construction.
:::



## Public monitor (YouGov) pipeline

:::{warning}
The YouGov pipeline is under construction.
:::

