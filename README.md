# Synopcsv

## Description
This package provides download and basic parsing for CSV files based on SYNOP code provided by Meteo France, as well as stations.
The base page is located at: https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32 

**This is not a synop parser**, as the files care in csv format, but contain synop codes, so the page is a bit misleading.

## Command line

A command line utility is available to download and insert some data into an influx db database, this is a test and work in progress.