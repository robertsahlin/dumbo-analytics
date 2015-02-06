register lookup.jar;
--define MetroResolver dumbo.pig.util.LookingAtLookup('lookup/cityResolver.csv');
--define MetroResolver dumbo.pig.util.LookingAtLookup('asv://lookup@tcneprod.blob.core.windows.net/cityResolver.csv');
define MetroResolver dumbo.pig.util.LookingAtLookup('http://tcneprod.blob.core.windows.net/lookup/cityResolver.csv');
A = load 'lookup/city.tsv' as (city:chararray);
B = foreach A generate city, MetroResolver(city) as country;
dump B;