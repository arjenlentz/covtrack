<?php

define ('TIMESERIES_PATH'       , '/home/arjen/Downloads/git/COVID-19/csse_covid_19_data/csse_covid_19_time_series/');

define ('TIMESERIES_CONFIRMED'  , 'time_series_19-covid-Confirmed.csv');
define ('TIMESERIES_DEATHS'     , 'time_series_19-covid-Deaths.csv');
define ('TIMESERIES_RECOVERED'  , 'time_series_19-covid-Recovered.csv');


function read_timeseries_csv($fname)
{
    $data = array();

    $fp = fopen($fname, 'r');
    if (!$fp)
        die("Can't open '$fname'\n");

    $header = fgetcsv($fp);
    foreach ($header as $key => $value) {
        if (is_numeric($value[0])) {
            // convert mm/dd/yy to standard ISO yyyy-mm-dd
            $d = strptime($value, '%m/%d/%y');
            $header[$key] = sprintf('%04u-%02u-%02u', $d['tm_year']+1900, $d['tm_mon']+1, $d['tm_mday']);
        }
    }
    //print_r($header);
    $data[] = $header;

    while (!feof($fp)) {
        $arr = fgetcsv($fp);
        if (is_null($arr) || count($arr) < 5)
            continue;
        //print_r($arr);
        $data[] = $arr;
    }

    fclose($fp);
    return ($data);
}


$confirmed = read_timeseries_csv(TIMESERIES_PATH.TIMESERIES_CONFIRMED);
$deaths = read_timeseries_csv(TIMESERIES_PATH.TIMESERIES_DEATHS);
$recovered = read_timeseries_csv(TIMESERIES_PATH.TIMESERIES_RECOVERED);

printf("confirmed=%u lines, deaths=%u lines, recovered=%u lines\n",
    count($confirmed), count($deaths), count($recovered));


// end of file