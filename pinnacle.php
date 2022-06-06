<?php
//Calling the minMax function to obtain minumum and maximum number of certified beds
//We are doing this first so we can set our buckets and then read the CSV in Batches to avoid memory issues
$minMax = minMax('https://data.cms.gov/provider-data/sites/default/files/resources/f7352642f405141988dc0ca9d1a0f6a5_1652385923/NH_ProviderInfo_May2022.csv');
$min_beds = $minMax[0];
$max_beds = $minMax[1];
$lines_count = $minMax[2];

//defining our 3 buckets 
$small_bucket_max = $max_beds * (34 / 100);
$medium_bucket_max = $max_beds * (67 / 100);
$large_bucket_max = $max_beds;

//defining few array that we are going to use 
$rows = array();
$small_bucket_array = array();
$medium_bucket_array = array();
$large_bucket_array = array();

//looping over batches of 1000
for ($start_position = 0; $start_position < $lines_count;){

    $rows[] = csv_batch("https://data.cms.gov/provider-data/sites/default/files/resources/f7352642f405141988dc0ca9d1a0f6a5_1652385923/NH_ProviderInfo_May2022.csv",$start_position,1000);
    $start_position += 1000;
    if($rows){
        $rows = $rows[0];
        $rows_array = (array) $rows;
    }else{
        echo "There is an issue getting the CSV batch";
        return;
    }

    for ($i = 0; $i < count($rows); $i++) {
        $record = (array) $rows_array[$i];
        //assigning records to their suitable buckets 
        if ($record["Number of Certified Beds"] < $small_bucket_max){  
            $small_bucket_array[] = [$record["Provider Name"], $record["Number of Certified Beds"]];
        } elseif ($record["Number of Certified Beds"] > $small_bucket_max && $record["Number of Certified Beds"] < $medium_bucket_max){
            $medium_bucket_array[] = [$record["Provider Name"], $record["Number of Certified Beds"]];
        } elseif ($record["Number of Certified Beds"] > $medium_bucket_max) {
            $large_bucket_array[] = [$record["Provider Name"], $record["Number of Certified Beds"]];
        }
    }
    //here unsetting the array after processing each batch to that momory can be used by the next batch
    unset($rows);
}
//calling the createCsv function to sort the arrays and create our final CSVs
createCsv($small_bucket_array,$medium_bucket_array,$large_bucket_array);


function createCsv($small_bucket_array,$medium_bucket_array,$large_bucket_array){
    // sorting ascending by bed count, and then by provider name
    $provider  = array_column($small_bucket_array, 0);
    $beds = array_column($small_bucket_array, 1);
    array_multisort($beds, SORT_ASC,$provider, SORT_ASC ,$small_bucket_array);
    array_unshift($small_bucket_array, array("Provider Name", " Number of Certified Beds"));

    //Creating CSV from the array
    $fp = fopen('small-providers.csv', 'w');
    foreach ($small_bucket_array as $item) {
        fputcsv($fp, $item);
    }
    fclose($fp);
    
    if (file_exists('small-providers.csv')) {
        echo "The file small-providers.csv is created" . PHP_EOL;
    } else {
        echo "The file small-providers.csvdoes not exist" . PHP_EOL;
    }

    // sorting ascending by bed count, and then by provider name
    $provider  = array_column($medium_bucket_array, 0);
    $beds = array_column($medium_bucket_array, 1);
    array_multisort($beds, SORT_ASC,$provider, SORT_ASC ,$medium_bucket_array);
    array_unshift($medium_bucket_array, array("Provider Name", " Number of Certified Beds"));

    //Creating CSV from the array
    $fp = fopen('medium-providers.csv', 'w');
    foreach ($medium_bucket_array as $item) {
        fputcsv($fp, $item);
    }
    fclose($fp);
    if (file_exists('medium-providers.csv')) {
        echo "The file medium-providers.csv is created" . PHP_EOL;
    } else {
        echo "The file medium-providers.csv does not exist" . PHP_EOL;
    }


    $fp = fopen('large-providers.csv', 'w');
    $provider  = array_column($large_bucket_array, 0);
    $beds = array_column($large_bucket_array, 1);
    array_multisort($beds, SORT_ASC,$provider, SORT_ASC ,$large_bucket_array);
    array_unshift($large_bucket_array, array("Provider Name", " Number of Certified Beds"));

    foreach ($large_bucket_array as $item) {
        fputcsv($fp, $item);
    }
    fclose($fp);
    if (file_exists('large-providers.csv')) {
        echo "The file large-providers.csv is created" . PHP_EOL;
    } else {
        echo "The file large-providers.csv does not exist" . PHP_EOL;
    }
}


//Here is the fucntion to get max and min of certified beds 
function minMax($filename){
    $min = 100000;
    $max = null;

    $fp = file($filename);
    $lines_count = count($fp);

    $file = fopen($filename, 'r');
    $seperator =  ",";

    for ($i=0;$i < $lines_count - 1; $i++){
        $row = fgetcsv($file, 0, $seperator);
        if ($i > 0){
            if (!empty($row)) {
                if ($row[10] > $max){
                    $max = $row[10];
                } elseif ($row[10] < $min){
                    $min = $row[10];
                }
            }
        }
    }
    return array($min, $max, $lines_count);
}

//Here passing in the file, staring position and batch chunk size and should get the rows of the csv back 
function csv_batch($filename, $start, $chunk_size) {
   
    $row = 0;
    $count = 0;
    $rows = array();

    if ($start == 0){
        $chunk_size -= 1;
    }

    if (($handle = fopen($filename, "r")) === FALSE) {
      return FALSE;
    }
    while (($row_data = fgetcsv($handle, 2000, ",")) !== FALSE) {
        //Here we are getting the headers.
        if ($row == 0) {
            $headers = $row_data;
            $row++;
            continue;
        } elseif ($row++ < $start) {
            continue;
        }
        //This is so we don't get than error for the final round when combining arrays below 
        if (count($headers) != count($row_data)){
            continue;
        }
        $rows[] = (object) array_combine($headers, $row_data);
        $count++;
        if ($count == $chunk_size) {   
            return $rows;
        }
    }
    return $rows;
  }

