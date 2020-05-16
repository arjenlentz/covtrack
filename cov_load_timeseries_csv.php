#!/usr/bin/php
<?php
/*
    COVtrack
    Copyright (C) 2020 by Arjen Lentz <arjen@lentz.com.au>
    Licensed under GPLv3

    Tracking data from https://github.com/CSSEGISandData/COVID-19
*/

define ('TIMESERIES_CONFIRMED'  , 'time_series_covid19_confirmed_global.csv');
define ('TIMESERIES_DEATHS'     , 'time_series_covid19_deaths_global.csv');
define ('TIMESERIES_RECOVERED'  , 'time_series_covid19_recovered_global.csv');

define ('DB_NAME', 'covtrack');



function exception_error_handler($errno, $errstr, $errfile, $errline ) {
    debug_print_backtrace();
    throw new ErrorException($errstr, $errno, 0, $errfile, $errline);
}
set_error_handler("exception_error_handler");



function read_timeseries_csv($fname)
{
    $data = array();

    $fp = @fopen($fname, 'r');
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
    $data['header'] = $header;

    while (!feof($fp)) {
        $arr = fgetcsv($fp);
        if (is_null($arr) || !is_array($arr) || count($arr) < 5)
            continue;

        // Dirty data: contains a stateprov "Recovered" for confirmed/deaths files for Canada
        if ($arr[0] == 'Recovered')
            continue;

        // We have data from County (US only), State/Province (Australia,Canada,China,US), and Country.
        // If we tally all that in the same table, we'll be double-counting people!
        // To fix this, we will:

        // - Not import the County-level data at all.
        if ($arr[1] == 'US' && ($arr[0] == 'US' || strchr($arr[0],',')))
            continue;

        // - Import State/Province-level data in a separate table.
        // We handle (later) that during processing.
 
        // Some countries also have external territories, but they're not like a province or county so we keep them separate.
        // In our input data for the main country of those situations, the stateprov is same as country, so we clear that.
        // That way all countries will look the same
        if ($arr[0] == $arr[1])
            $arr[0] = '';

        $data[trim($arr[0] . ' ' . $arr[1])] = $arr;
    }

    fclose($fp);
    return ($data);
}



// -------------------------------------
// get cmdline options

$shortopts = 'f:h:u:p:';    // filepath, dbhost, user, pwd

$options = getopt($shortopts);
$filepath = isset($options['f']) ? $options['f'] : './';           // default filepath to current dir
$db_host = isset($options['h']) ? $options['h'] : 'localhost';     // default dbhost to localhost
if (!isset($options['u']))
    die("Missing -u (db user) option\n");
$db_user = $options['u'];
$db_pass = isset($options['p']) ? $options['p'] : NULL;            // default user to no pwd



// -------------------------------------
// Read latest timeseries CSVs.
$confirmed = read_timeseries_csv($filepath.TIMESERIES_CONFIRMED);
$deaths = read_timeseries_csv($filepath.TIMESERIES_DEATHS);
$recovered = read_timeseries_csv($filepath.TIMESERIES_RECOVERED);

// Grab header row, then remove it from dataset arrays
$header = $confirmed['header'];
unset($confirmed['header']);
unset($deaths['header']);
unset($recovered['header']);


function aggregate_stateprov_to_country ($country, &$dataset)
{
    $dataset[$country][0] = '';         // stateprov
    $dataset[$country][1] = $country;
    $dataset[$country][2] = 0;          // lat (yep so we lose that info on aggregation)
    $dataset[$country][3] = 0;          // lon

    foreach ($dataset as $key => $row) {
        if ($row[1] != $country || empty($row[0]))
            continue;

        for ($i = 4; $i < count($row); $i++) {
            if (!isset($dataset[$country][$i]))
                $dataset[$country][$i] = $row[$i];
            else
                $dataset[$country][$i] += $row[$i];
        }

        unset($dataset[$key]);
    }
}

aggregate_stateprov_to_country('Canada',$confirmed);
aggregate_stateprov_to_country('Canada',$deaths);


// Because we're processing all these three datasets in parallel,
// we have to ensure they're same # cols, as it should be!
// Not an ideal input format really, but if we check it should be ok.
// They don't have the same country on the same row, so we do that differently already.

/* No need to check this, we just run on any country that has confirmed cases
$rows = count($confirmed);
if ($rows != count($deaths) || $rows != count($recovered))
    die("The timeseries CSV files have different number of rows\n");
*/

$cols = count($header);
foreach ($confirmed as $key => $row) {
/*
    Don't need to catch these, we'll deal with them on the fly
    if (!in_array($key, $deaths))
        print("Location '$key' from confirmed not in deaths\n");
    if (!in_array($key, $recovered))
        print("Location '$key' from confirmed not in recovered\n");
*/
    if ($cols != count($confirmed[$key]) || $cols != count($deaths[$key]) || $cols != count($recovered[$key])) {
        die("Different number of columns on timeseries row '$key'\n");
    }
}



// -------------------------------------
// Open db
$db = new mysqli($db_host, $db_user, $db_pass, DB_NAME);
if ($db->connect_error) {
    die('Connect Error (' . $db->connect_errno . ') ' . $db->connect_error);
}

$create_locations_table_query = '
    CREATE TABLE IF NOT EXISTS locations (
        id          INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        stateprov   VARCHAR(50) NOT NULL,
        country     VARCHAR(50) NOT NULL,
        lat         DECIMAL(7,4),
        lon         DECIMAL(7,4),
        UNIQUE KEY  (stateprov,country),
        INDEX       (country)
    ) ENGINE=InnoDB
';
$db->query($create_locations_table_query);


$db->begin_transaction();

$put_location_query = 'INSERT IGNORE INTO locations (stateprov,country,lat,lon) VALUES (?,?,?,?)';
$put_location_stmt = $db->prepare($put_location_query);

// step through countries
foreach ($confirmed as $key => $row) {
    $location_stateprov = $confirmed[$key][0];
    $location_country = $confirmed[$key][1];
    $location_lat = $confirmed[$key][2];
    $location_lon = $confirmed[$key][3];

    $put_location_stmt->bind_param('ssss', $location_stateprov, $location_country, $location_lat, $location_lon);
    $put_location_stmt->execute();
}
$put_location_stmt->close();

$db->commit();


// creating a lookup array for stateprov/country -> id
$get_location_query = 'SELECT id,stateprov,country FROM locations';
$get_location_stmt = $db->prepare($get_location_query);
$get_location_stmt->execute();
$get_location_stmt->bind_result($location_id, $location_stateprov, $location_country);
$location_lookup = array();
while ($get_location_stmt->fetch()) {
    $key = trim($location_stateprov . ' ' . $location_country);
    $location_lookup[$key] = $location_id;
}
$get_location_stmt->close();
ksort($location_lookup);    // sort array by key, predictable processing order


$create_items_table_query = '
    CREATE TABLE items (
        id                  INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        location_id         INT UNSIGNED NOT NULL,
        recdate             DATE,
        confirmed_total     INT UNSIGNED NOT NULL,
        deaths_total        INT UNSIGNED NOT NULL,
        recovered_total     INT UNSIGNED NOT NULL,
        confirmed_new       INT UNSIGNED NOT NULL,
        deaths_new          INT UNSIGNED NOT NULL,
        recovered_new       INT UNSIGNED NOT NULL,
        confirmed_active    INT UNSIGNED NOT NULL,
        UNIQUE KEY          (location_id,recdate),
        INDEX               (recdate)
    ) ENGINE=InnoDB
';
$db->query($create_items_table_query);


$db->begin_transaction();

// try and insert, or update the data within the key (location_id,recdate)
$put_item_query = 'INSERT INTO items (location_id,recdate,confirmed_total,deaths_total,recovered_total,confirmed_new,deaths_new,recovered_new,confirmed_active)'
            . ' VALUES (?,?,?,?,?,?,?,?,?)'
            . ' ON DUPLICATE KEY UPDATE'
                    . ' confirmed_total=?, deaths_total=?, recovered_total=?,'
                    . ' confirmed_new=?, deaths_new=?, recovered_new=?,'
                    . ' confirmed_active=?';
$put_item_stmt = $db->prepare($put_item_query);

// step through countries
foreach ($confirmed as $key => $row) {
    if (!array_key_exists($key, $location_lookup))
        die("Location key '$key' not find in location lookup array\n");
    $location_id = $location_lookup[$key];

    $last_confirmed_total = $last_deaths_total = $last_recovered_total = 0;
    // step through dates within this country
    for ($col = 4; $col < $cols; $col++) {
        // grab date of this column
        $recdate = $header[$col];

        // the numbers from the timeseries CSVs
        // dirty dataset from 2020-03-23: some columns empty rather than 0
        $confirmed_total    = is_numeric($row[$col]) ? $row[$col] : 0;
        $deaths_total       = array_key_exists($key, $deaths) && is_numeric($deaths[$key][$col]) ? $deaths[$key][$col] : 0;
        $recovered_total    = array_key_exists($key, $recovered) && is_numeric($recovered[$key][$col]) ? $recovered[$key][$col] : 0;

        // calculate some extra data while we're here
        $confirmed_new      = $confirmed_total  - $last_confirmed_total;
        $deaths_new         = $deaths_total     - $last_deaths_total;
        $recovered_new      = $recovered_total  - $last_recovered_total;

        $confirmed_active   = $confirmed_total - ($deaths_total + $recovered_total);

        $put_item_stmt->bind_param('isiiiiiiiiiiiiii', $location_id, $recdate,
                                    $confirmed_total, $deaths_total, $recovered_total, $confirmed_new, $deaths_new, $recovered_new, $confirmed_active,
                                    $confirmed_total, $deaths_total, $recovered_total, $confirmed_new, $deaths_new, $recovered_new, $confirmed_active
                                );
        $put_item_stmt->execute();

        $last_confirmed_total   = $confirmed_total;
        $last_deaths_total      = $deaths_total;
        $last_recovered_total   = $recovered_total;
    }
}
$put_item_stmt->close();
$db->commit();

$db->close();


// end of file