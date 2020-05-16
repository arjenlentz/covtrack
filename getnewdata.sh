cd ../COVID-19
git pull
cd ../covtrack
time ./cov_load_timeseries_csv.php -u covtrack -p covtrack -f ../COVID-19/csse_covid_19_data/csse_covid_19_time_series/
