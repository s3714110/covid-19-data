# Initialization
bash config.sh

# Make sure we have the latest commit.
git checkout $BRANCH && git pull


# Run Grapher updates
cowidev-grapher-db

## Additional individual grapher updates
minute=$(date +%M)
if [ $minute == 20 ] ; then
  cowid --server jhu grapher-db
  cowid --server decoupling grapher-db
  cowid --server hosp grapher-db
  cowid --server gmobility grapher-db
  cowid --server sweden grapher-db
  cowid --server uk-nations grapher-db
  cowid --server oxcgrt grapher-db
fi