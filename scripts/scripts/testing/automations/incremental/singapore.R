url <- "https://www.moh.gov.sg/covid-19/statistics"

page <- read_html(url)

date <- page %>% html_nodes("h3") %>%
    html_text() %>%
    str_subset("Tested") %>%
    str_extract("as .. [^)]+") %>%
    str_replace("as .. ", "") %>%
    na.omit() %>%
    dmy()

count <- page %>%
    html_nodes("#ContentPlaceHolder_contentPlaceholder_C030_Col00 td") %>%
    html_text() %>%
    tail(1) %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Singapore",
    country = "Singapore",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)
