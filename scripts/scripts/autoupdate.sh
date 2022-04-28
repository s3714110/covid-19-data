#!/bin/bash

set -e

BRANCH="master"
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
SCRIPTS_DIR=$ROOT_DIR/scripts

# ENV VARS
export OWID_COVID_PROJECT_DIR=${ROOT_DIR}
export OWID_COVID_CONFIG=${OWID_COVID_PROJECT_DIR}/scripts/config.yaml
export OWID_COVID_SECRETS=${OWID_COVID_PROJECT_DIR}/scripts/secrets.yaml
export PATH=$PATH:/usr/local/bin/  # so geckodriver is correctly found

has_changed() {
  git diff --name-only --exit-code $1 >/dev/null 2>&1
  [ $? -ne 0 ]
}

has_changed_gzip() {
  # Ignore the header because it includes the creation time
  cmp --silent -i 8 $1 <(git show HEAD:$1)
  [ $? -ne 0 ]
}

git_push() {
  git add .
  git commit -m $1
  git push
}

cd $ROOT_DIR

# Activate Python virtualenv
source $SCRIPTS_DIR/venv/bin/activate

# Interpret inline Python script in `scripts` directory
run_python() {
  (cd $SCRIPTS_DIR/scripts; python -c "$1")
}

# Make sure we have the latest commit.
git reset --hard origin/master
git pull

# =====================================================================
# JHU

# Attempt to download JHU CSVs
# run_python 'import jhu; jhu.download_csv()'
cowid jhu get

# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
hour=$(date +%H)
if [ $hour == 00 ] || [ $hour == 02 ] || [ $hour == 04 ] || [ $hour == 06 ] || [ $hour == 08 ] || [ $hour == 10 ] || [ $hour == 12 ] || [ $hour == 14 ] ||[ $hour == 16 ] || [ $hour == 18 ] || [ $hour == 20 ] || [ $hour == 22 ]; then
  if has_changed './scripts/input/jhu/*'; then
    echo "Generating JHU files..."
    cowid --server jhu generate
    cowid --server megafile
    # python $SCRIPTS_DIR/scripts/jhu.py --skip-download
    git_push "data(jhu): automated update"
  else
    echo "JHU export is up to date"
  fi
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
cowid jhu grapher-db
# run_python 'import jhu; jhu.update_db()'

# =====================================================================
# Decoupling charts

hour=$(date +%H)
if [ $hour == 01 ] ; then
  echo "Generating decoupling dataset..."
  run_python 'import decoupling; decoupling.main()'
  git_push "data(decoupling): automated update"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import decoupling; decoupling.update_db()'

# =====================================================================
# VAX ICER
# This basically download the vaccination data needed for some countries
# The idea is that here we put extremely slow scripts, so their updates are managed separately
hour=$(date +%H)
if [ $hour == 03 ] ; then
  echo "Generating ICE vaccination data..."
  cowid vax icer
fi

# =====================================================================
# Hospital & ICU data

hour=$(date +%H)
if [ $hour == 05 ] || [ $hour == 17 ] ; then
  # Download CSV
  echo "Generating hospital & ICU export..."
  cowid --server hosp generate
  cowid --server hosp grapher-io
  cowid --server megafile
  git_push "data(hosp): automated update"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
cowid hosp grapher-db

# =====================================================================
# Vaccinations

hour=$(date +%H)
if [ $hour == 07 ] ; then
  echo "Generating Vaccination (get, process, generate) & megafile..."
  cowid --server vax get
  cowid --server vax process generate
  cowid --server megafile
  git_push "data(vax): automated update"
fi


# =====================================================================
# Google Mobility

hour=$(date +%H)
if [ $hour == 09 ] ; then

  # Download CSV
  cowid gmobility generate

  echo "Generating Google Mobility export..."
  cowid gmobility grapher-io

  if has_changed './scripts/grapher/Google Mobility Trends (2020).csv'; then
    git_push "data(mobility): automated update"
  fi

fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
cowid gmobility grapher-db

# =====================================================================
# Swedish Public Health Agency

# Attempt to download data
run_python 'import sweden; sweden.download_data()'

# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
hour=$(date +%H)
if [ $hour == 11 ] ; then
  if has_changed './scripts/input/sweden/sweden_deaths_per_day.csv'; then
    echo "Generating Swedish Public Health Agency dataset..."
    run_python 'import sweden; sweden.generate_dataset()'
    git_push "data(sweden): automated update"
  else
    echo "Swedish Public Health Agency export is up to date"
  fi
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import sweden; sweden.update_db()'

# =====================================================================
# UK subnational data

hour=$(date +%H)
if [ $hour == 13 ] ; then
  # Download CSV
  echo "Generating UK subnational export..."
  run_python 'import uk_nations; uk_nations.generate_dataset()'
  git_push "data(uk): automated update"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
run_python 'import uk_nations; uk_nations.update_db()'

# =====================================================================
# US vaccinations

# # Attempt to download CDC data
# run_python 'import us_vaccinations; us_vaccinations.download_data()'

# # If there are any unstaged changes in the repo, then one of
# # the CSVs has changed, and we need to run the update script.
# echo "Generating US vaccination file..."
# run_python 'import us_vaccinations; us_vaccinations.generate_dataset()'
# if has_changed './public/data/vaccinations/us_state_vaccinations.csv'; then
#   git add .
#   git commit -m "Automated US vaccination update"
#   git push
#   run_python 'import us_vaccinations; us_vaccinations.update_db()'
# else
#   echo "US vaccination export is up to date"
# fi

hour=$(date +%H)
if [ $hour == 15 ] ; then
  echo "Generating US vaccination files..."
  python -m cowidev.vax.us_states etl
  python -m cowidev.vax.us_states grapher-file
  if has_changed './public/data/vaccinations/us_state_vaccinations.csv'; then
    git_push "data(vax,us): automated update"
  else
    echo "US vaccination export is up to date"
  fi
fi

# =====================================================================
# Variants
# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
hour=$(date +%H)
if [ $hour == 19 ] ; then
  echo "Generating CoVariants dataset..."
  cowid variants generate
  cowid variants grapher-io
  git_push "data(variants): automated update"
fi


# =====================================================================
# Excess Mortality
# If there are any unstaged changes in the repo, then one of
# the CSVs has changed, and we need to run the update script.
hour=$(date +%H)
if [ $hour == 21 ] ; then
  echo "Generating CoVariants dataset..."
  cowid xm generate
  cowid --server megafile
  git_push "data(xm): automated update"
fi

# =====================================================================
# Policy responses

# The policy update files change far too often (every hour or so).
# We don't want to run an update if one has already been run in the
# last 6 hours.

OXCGRT_CSV_PATH=./scripts/input/bsg/latest.csv
hour=$(date +%H)
if [ $hour == 23 ] ; then
  # Download CSV
  cowid --server oxcgrt get
  # If there are any unstaged changes in the repo, then the
  # CSV has changed, and we need to run the update script.
  if has_changed $OXCGRT_CSV_PATH; then
    echo "Generating OxCGRT export..."
    cowid --server oxcgrt grapher-io
    git_push "data(oxcgrt): automated update"
  else
    echo "OxCGRT export is up to date"
  fi
else
  echo "OxCGRT CSV was recently updated; skipping download"
fi

# Always run the database update.
# The script itself contains a check against the database
# to make sure it doesn't run unnecessarily.
# run_python 'import oxcgrt; oxcgrt.update_db()'
cowid --server oxcgrt grapher-db
