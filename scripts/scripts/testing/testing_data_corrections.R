# Austria: The definition of the tests in this dataset was changed on 13 January 2021 to include
# both antigen and PCR tests. Because historical testing data was added on that date, the timeline
# cannot be used for continuous calculations. Like most Austrian media outlets and scientific
# institutions, we have therefore changed our test positive rate calculations to only include data
# from February 2021 onwards.
collated[Country == "Austria" & Date < "2021-02-01", `Short-term positive rate` := NA]
collated[Country == "Austria" & Date < "2021-02-01", `Short-term tests per case` := NA]

# Ecuador: the PR should start from Sept 2020, because the case data prior to that
# included cases confirmed without PCR tests (while our data only includes PCR tests)
collated[Country == "Ecuador" & Date < "2020-09-14", `Short-term positive rate` := NA]
collated[Country == "Ecuador" & Date < "2020-09-14", `Short-term tests per case` := NA]

# Mauritania: the test definition does not match the case definition (screening tests possibly included in testing figures)
collated[Country == "Mauritania", `Short-term positive rate` := NA]
collated[Country == "Mauritania", `Short-term tests per case` := NA]

# Lebanon: the test definition does not match the case definition (testing figures exclude antigen tests, which can be used to diagnose cases of COVID-19)
collated[Country == "Lebanon", `Short-term positive rate` := NA]
collated[Country == "Lebanon", `Short-term tests per case` := NA]

# Iceland: the test definition does not match the case definition (confirmed cases does not exclude positive results from tests included in the testing figure)
collated[Country == "Iceland", `Short-term positive rate` := NA]
collated[Country == "Iceland", `Short-term tests per case` := NA]

# Qatar: the test definition does not match the case definition (confirmed cases does not exclude positive results from tests included in the testing figure)
collated[Country == "Qatar", `Short-term positive rate` := NA]
collated[Country == "Qatar", `Short-term tests per case` := NA]

# Norway:  Only those who test positive with the self-tests and who then take a confirmatory PCR test are registered in the MSIS lab database. 
collated[Country == "Norway", `Short-term positive rate` := NA]
collated[Country == "Norway", `Short-term tests per case` := NA]