### This code was developed to perform AVL/APC data processing and analysis for the paper
### "Empirical analysis of battery-electric bus transit operations in Portland, OR, USA" 
### The paper is available at https://doi.org/10.1016/j.trd.2024.104120

### The code was developed by Bogdan Kapatsila in Spring-Summer 2022

rm(list = ls())
#rm(list=setdiff(ls(), "trimet_single")) # When need to remove everything but one df

par(mfrow = c(1,1))

library(tidyverse)
library(data.table)

#library(ggplot2)
#library(sqldf)
#library(tidytransit)

#library(gtfstools)
#library(readxl)
#library(dplyr)
#library(magrittr)
#library(stargazer)

#library(pivottabler)

#library(mlogit)

#setwd("D:/Research/2022_Trimet/Data")

setwd("D:/Research/2022_Trimet/Data_2")
#write("TMP = 'C:/Users/bokap/AppData/Local/Temp'", file=file.path(Sys.getenv('R_USER'), '.Renviron'))

### GTFS stops for routes ========================================================
trimet.gtfs <- read_gtfs('gtfs_March.zip')

# Logic - https://www.adventuremeng.com/post/tidytransit-linking-gtfs-stop-ids-and-routes/

routes <- c('6', '8', '20', '62')
trimet.routes <- trimet.gtfs$routes
trimet.routes <- filter(trimet.routes, route_id %in% routes)

trimet.directions <- trimet.gtfs$route_directions
trimet.route.dir <- merge(trimet.routes, trimet.directions, by='route_id')

trimet.trips <- trimet.gtfs$trips
trimet.route.dir.trip <- merge(trimet.route.dir, trimet.trips, by='route_id')

trimet.stoptimes <- trimet.gtfs$stop_times
trmiet.route.dir.trip.stime <- merge(trmiet.route.dir.trip, trimet.stoptimes, by='trip_id')

trimet.stops <- trimet.gtfs$stops
trimet.route.dir.trip.stime.stop <- merge(trmiet.route.dir.trip.stime, trimet.stops, by='stop_id')

route_6 <- filter(trimet.route.dir.trip.stime.stop, route_id=='6') #53 stops
table(route_6$trip_id)
route_6_stops_0 <- route_6 %>% filter(trip_id=='11358562') %>% filter(direction_id.x=='0')
route_6_stops_1 <- route_6 %>% filter(trip_id=='11358562') %>% filter(direction_id.x=='1')

route_8 <- filter(trimet.route.dir.trip.stime.stop, route_id=='8') #50 stops
table(route_8$trip_id)
route_8_stops_0 <- route_8 %>% filter(trip_id=='11421213') %>% filter(direction_id.x=='0')
route_8_stops_1 <- route_8 %>% filter(trip_id=='11421213') %>% filter(direction_id.x=='1')

route_20 <- filter(trimet.route.dir.trip.stime.stop, route_id=='20') #135 stops
table(route_20$trip_id)
route_20_stops_0 <- route_20 %>% filter(trip_id=='11423608') %>% filter(direction_id.x=='0')
route_20_stops_1 <- route_20 %>% filter(trip_id=='11423608') %>% filter(direction_id.x=='1')

route_62 <- filter(trimet.route.dir.trip.stime.stop, route_id=='62') #56 stops
table(route_62$trip_id)
route_62_stops_0 <- route_62 %>% filter(trip_id=='11426865') %>% filter(direction_id.x=='0')
route_62_stops_1 <- route_62 %>% filter(trip_id=='11426865') %>% filter(direction_id.x=='1')


### READ THE GIANT CSV =========================================================

# Tutorial: https://www.michaelc-m.com/manual_posts/2022-01-27-big-CSV-SQL.html
library(readr)
library(DBI)
library(RSQLite)

# Read 20 records and a header
trimet_20 <- read.csv("bus_service_sept_2021.csv", nrows=20)

# Create an SQL database
trimetdb <- dbConnect(SQLite(), "trimetdb")

# Read the SQL database (only the first time)
read_csv_chunked("bus_service_sept_2021.csv", 
                 callback = function(chunk, dummy){
                   dbWriteTable(trimetdb, "trimet", chunk, append = T)}, 
                 chunk_size = 100000, col_types = "ciiiiiciiiiiiiiiininnniiciccnnnc") # https://readr.tidyverse.org/reference/read_delim_chunked.html

# Create a tibble 
trimet.base <- tbl(trimetdb, "trimet")

# Create lists of vehicle numbers
trimet.buses <- read.csv('Variables/trimet_bus_type.csv')
trimet.buses <- subset(trimet.buses, select=c(VEHICLE_NUMBER))
#trimet.buses <- trimet.buses[order(trimet.buses$VEHICLE_NUMBER),]
#trimet.buses <- as.data.frame(trimet.buses)

trimet.buses.2801_3101 <- filter(trimet.buses, VEHICLE_NUMBER<=3101)
trimet.buses.2801_3101 <- array(trimet.buses.2801_3101$VEHICLE_NUMBER)
trimet.buses.2801_3101 <- filter(trimet.base, VEHICLE_NUMBER %in% trimet.buses.2801_3101)
trimet.buses.2801_3101.df <- as.data.frame(trimet.buses.2801_3101)
write.csv(trimet.buses.2801_3101.df, 'Parts/trimet.buses.2801_3101.csv')
rm(trimet.buses.2801_3101, trimet.buses.2801_3101.df)
gc()

trimet.buses.3102_3250 <- filter(trimet.buses, VEHICLE_NUMBER>=3102 & VEHICLE_NUMBER<=3250)
trimet.buses.3102_3250 <- array(trimet.buses.3102_3250$VEHICLE_NUMBER)
trimet.buses.3102_3250 <- filter(trimet.base, VEHICLE_NUMBER %in% trimet.buses.3102_3250)
trimet.buses.3102_3250.df <- as.data.frame(trimet.buses.3102_3250)
write.csv(trimet.buses.3102_3250.df, 'Parts/trimet.buses.3102_3250.csv')
rm(trimet.buses.3102_3250, trimet.buses.3102_3250.df)
gc()

trimet.buses.3251_3620 <- filter(trimet.buses, VEHICLE_NUMBER>=3251 & VEHICLE_NUMBER<=3620)
trimet.buses.3251_3620 <- array(trimet.buses.3251_3620$VEHICLE_NUMBER)
trimet.buses.3251_3620 <- filter(trimet.base, VEHICLE_NUMBER %in% trimet.buses.3251_3620)
trimet.buses.3251_3620.df <- as.data.frame(trimet.buses.3251_3620)
write.csv(trimet.buses.3251_3620.df, 'Parts/trimet.buses.3251_3620.csv')
rm(trimet.buses.3251_3620, trimet.buses.3251_3620.df)
gc()

trimet.buses.3621_3940 <- filter(trimet.buses, VEHICLE_NUMBER>=3621 & VEHICLE_NUMBER<=3940)
trimet.buses.3621_3940 <- array(trimet.buses.3621_3940$VEHICLE_NUMBER)
trimet.buses.3621_3940 <- filter(trimet.base, VEHICLE_NUMBER %in% trimet.buses.3621_3940)
trimet.buses.3621_3940.df <- as.data.frame(trimet.buses.3621_3940)
write.csv(trimet.buses.3621_3940.df, 'Parts/trimet.buses.3621_3940.csv')
rm(trimet.buses.3621_3940, trimet.buses.3621_3940.df)
gc()

trimet.buses.3941_4305 <- filter(trimet.buses, VEHICLE_NUMBER>=3941)
trimet.buses.3941_4305 <- array(trimet.buses.3941_4305$VEHICLE_NUMBER)
trimet.buses.3941_4305 <- filter(trimet.base, VEHICLE_NUMBER %in% trimet.buses.3941_4305)
trimet.buses.3941_4305.df <- as.data.frame(trimet.buses.3941_4305)
write.csv(trimet.buses.3941_4305.df, 'Parts/trimet.buses.3941_4305.csv')
rm(trimet.buses.3941_4305, trimet.buses.3941_4305.df)
gc()

 
#trimet_2805 <- trimet.base %>% filter(VEHICLE_NUMBER == '2805')
#trimet_2805 <- as.data.frame(trimet_2805)
#write.csv(trimet_2805, 'trimet_2805.csv')

### PREPARE BREAK DATA =========================================================

trimet_breaks <- read.csv("vehicle_fails_sept_2021.csv", header = TRUE, na.strings = c("","NA"))
names(trimet_breaks)[names(trimet_breaks)=='EQUIPMENT_COMPONENT_NUMBER'] <- 'VEHICLE_NUMBER'
names(trimet_breaks)[names(trimet_breaks)=='date_event'] <- 'DATE'

BREAK_OUT <- c('ACCIDENT',
               'BODY REPAIR',
               'CAD/AVL SYSTEM/RADIO',
               'CONVENIENCE',
               'FARE SYSTEM',
               'INTERIOR CLEANING',
               'LIGHTS (NON-WARNING)',
               'MEDICAL ISSUE',
               'OPERATOR/PASSENGER SEAT',
               'PASSENGER PROBLEM',
               'PASSENGER STOP REQUEST',
               'RADIO P.A. SYSTEM',
               'RADIO/ASA/PA SYSTEM',
               'VANDALISM',
               'WINDOW/WINDSHIELD/GLASS',
               'WIPER/WASHER SYSTEM')

trimet_breaks <- filter(trimet_breaks, !DESCRIPTION %in% BREAK_OUT) # Take out some of the breaks
break_sample <- distinct(trimet_breaks, VEHICLE_NUMBER, DATE, .keep_all= TRUE) # Remove when several breaks in one day

break_sample$DATE <- as.Date(break_sample$DATE, format = "%d-%b-%Y")
break_sample$SD_VN <- paste(break_sample$DATE, break_sample$VEHICLE_NUMBER, sep = '_')
break_sample$BREAK <- c(1)

write.csv(break_sample, 'break_all_clean.csv')

breaks_counts <- aggregate(BREAK ~ VEHICLE_NUMBER, data = break_sample, sum)
write.csv(breaks_counts, 'break_counts_all_clean.csv')

# First break in the raw
break_first <- data.table(break_sample, key = c("VEHICLE_NUMBER", "DATE")) #Sort by vehicle number and date of the break
break_first <- distinct(break_first, VEHICLE_NUMBER, .keep_all= TRUE)

write.csv(break_first, 'break_first.csv')

# Breaks in pieces
break_all <- read.csv('break_all_clean.csv', header = TRUE, na.strings = c("","NA"))
break_all$DATE_BREAK <- as.Date(break_all$DATE, format = "%Y-%m-%d")
break_all <- subset(break_all, select=c(VEHICLE_NUMBER, DATE_BREAK))
break_all$SD_VN <- paste(break_all$DATE_BREAK, break_all$VEHICLE_NUMBER, sep = '_')

break_first <- data.table(break_all, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_first <- distinct(break_first, VEHICLE_NUMBER, .keep_all= TRUE)
first_break <- array(break_first$SD_VN)

break_second <- filter(break_all, !SD_VN %in% first_break)
break_second <- data.table(break_second, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_second <- distinct(break_second, VEHICLE_NUMBER, .keep_all= TRUE)
second_break <- array(break_second$SD_VN)
first2_break <- c(first_break, second_break)

break_third <- filter(break_all, !SD_VN %in% first2_break)
break_third <- data.table(break_third, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_third <- distinct(break_third, VEHICLE_NUMBER, .keep_all= TRUE)
third_break <- array(break_third$SD_VN)
first23_break <- c(first2_break, third_break)

break_fourth <- filter(break_all, !SD_VN %in% first23_break)
break_fourth <- data.table(break_fourth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_fourth <- distinct(break_fourth, VEHICLE_NUMBER, .keep_all= TRUE)
fourth_break <- array(break_fourth$SD_VN)
first234_break <- c(first23_break, fourth_break)

break_fifth <- filter(break_all, !SD_VN %in% first234_break)
break_fifth <- data.table(break_fifth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_fifth <- distinct(break_fifth, VEHICLE_NUMBER, .keep_all= TRUE)
fifth_break <- array(break_fifth$SD_VN)
first2345_break <- c(first234_break, fifth_break)

break_sixth <- filter(break_all, !SD_VN %in% first2345_break)
break_sixth <- data.table(break_sixth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_sixth <- distinct(break_sixth, VEHICLE_NUMBER, .keep_all= TRUE)
sixth_break <- array(break_sixth$SD_VN)
first23456_break <- c(first2345_break, sixth_break)

break_seventh <- filter(break_all, !SD_VN %in% first23456_break)
break_seventh <- data.table(break_seventh, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_seventh <- distinct(break_seventh, VEHICLE_NUMBER, .keep_all= TRUE)
seventh_break <- array(break_seventh$SD_VN)
first234567_break <- c(first23456_break, seventh_break)

break_eighth <- filter(break_all, !SD_VN %in% first234567_break)
break_eighth <- data.table(break_eighth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_eighth <- distinct(break_eighth, VEHICLE_NUMBER, .keep_all= TRUE)
eighth_break <- array(break_eighth$SD_VN)
first2345678_break <- c(first234567_break, eighth_break)

break_ninth <- filter(break_all, !SD_VN %in% first2345678_break)
break_ninth <- data.table(break_ninth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_ninth <- distinct(break_ninth, VEHICLE_NUMBER, .keep_all= TRUE)
ninth_break <- array(break_ninth$SD_VN)
first23456789_break <- c(first2345678_break, ninth_break)

break_tenth <- filter(break_all, !SD_VN %in% first23456789_break)
break_tenth <- data.table(break_tenth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_tenth <- distinct(break_tenth, VEHICLE_NUMBER, .keep_all= TRUE)
tenth_break <- array(break_tenth$SD_VN)
first2345678910_break <- c(first23456789_break, tenth_break)

break_eleventh <- filter(break_all, !SD_VN %in% first2345678910_break)
break_eleventh <- data.table(break_eleventh, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_eleventh <- distinct(break_eleventh, VEHICLE_NUMBER, .keep_all= TRUE)
eleventh_break <- array(break_eleventh$SD_VN)
first234567891011_break <- c(first2345678910_break, eleventh_break)

break_twelfth <- filter(break_all, !SD_VN %in% first234567891011_break)
break_twelfth <- data.table(break_twelfth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_twelfth <- distinct(break_twelfth, VEHICLE_NUMBER, .keep_all= TRUE)
twelfth_break <- array(break_twelfth$SD_VN)
first23456789101112_break <- c(first234567891011_break, twelfth_break)

break_thirteenth <- filter(break_all, !SD_VN %in% first23456789101112_break)
break_thirteenth <- data.table(break_thirteenth, key = c("VEHICLE_NUMBER", "DATE_BREAK")) #Sort by vehicle number and date of the break
break_thirteenth <- distinct(break_thirteenth, VEHICLE_NUMBER, .keep_all= TRUE)
thirteenth_break <- array(break_thirteenth$SD_VN)
first2345678910111213_break <- c(first23456789101112_break, thirteenth_break)

break_remaining <- filter(break_all, !SD_VN %in% first2345678910111213_break)
break_fourteenth <- break_remaining[1,]
break_fifteenth <- break_remaining[2,]
break_sixteenth <- break_remaining[3,]
break_seventeenth <- break_remaining[4,]
break_eighteenth <- break_remaining[5,]
break_nineteenth <- break_remaining[6,]
break_twentieth <- break_remaining[7,]
break_twentyfirst <- break_remaining[8,]

break_first$B_OCCUR <- c(1)
break_second$B_OCCUR <- c(2)
break_third$B_OCCUR <- c(3)
break_fourth$B_OCCUR <- c(4)
break_fifth$B_OCCUR <- c(5)
break_sixth$B_OCCUR <- c(6)
break_seventh$B_OCCUR <- c(7)
break_eighth$B_OCCUR <- c(8)
break_ninth$B_OCCUR <- c(9)
break_tenth$B_OCCUR <- c(10)
break_eleventh$B_OCCUR <- c(11)
break_twelfth$B_OCCUR <- c(12)
break_thirteenth$B_OCCUR <- c(13)
break_fourteenth$B_OCCUR <- c(14)
break_fifteenth$B_OCCUR <- c(15)
break_sixteenth$B_OCCUR <- c(16)
break_seventeenth$B_OCCUR <- c(17)
break_eighteenth$B_OCCUR <- c(18)
break_nineteenth$B_OCCUR <- c(19)
break_twentieth$B_OCCUR <- c(20)
break_twentyfirst$B_OCCUR <- c(21)

break_numbered <- rbind(break_first,
                        break_second,
                        break_third,
                        break_fourth,
                        break_fifth,
                        break_sixth,
                        break_seventh,
                        break_eighth,
                        break_ninth,
                        break_tenth,
                        break_eleventh,
                        break_twelfth,
                        break_thirteenth,
                        break_fourteenth,
                        break_fifteenth,
                        break_sixteenth,
                        break_seventeenth,
                        break_eighteenth,
                        break_nineteenth,
                        break_twentieth,
                        break_twentyfirst)

write.csv(break_numbered, 'Just_Breaks/break_numbered.csv')
write.csv(break_first, 'Just_Breaks/break_first.csv')
write.csv(break_second, 'Just_Breaks/break_second.csv')
write.csv(break_third, 'Just_Breaks/break_third.csv')
write.csv(break_fourth, 'Just_Breaks/break_fourth.csv')
write.csv(break_fifth, 'Just_Breaks/break_fifth.csv')
write.csv(break_sixth, 'Just_Breaks/break_sixth.csv')
write.csv(break_seventh, 'Just_Breaks/break_seventh.csv')
write.csv(break_eighth, 'Just_Breaks/break_eighth.csv')
write.csv(break_ninth, 'Just_Breaks/break_ninth.csv')
write.csv(break_tenth, 'Just_Breaks/break_tenth.csv')
write.csv(break_eleventh, 'Just_Breaks/break_eleventh.csv')
write.csv(break_twelfth, 'Just_Breaks/break_twelfth.csv')
write.csv(break_thirteenth, 'Just_Breaks/break_thirteenth.csv')
write.csv(break_fourteenth, 'Just_Breaks/break_fourteenth.csv')
write.csv(break_fifteenth, 'Just_Breaks/break_fifteenth.csv')
write.csv(break_sixteenth, 'Just_Breaks/break_sixteenth.csv')
write.csv(break_seventeenth, 'Just_Breaks/break_seventeenth.csv')
write.csv(break_eighteenth, 'Just_Breaks/break_eighteenth.csv')
write.csv(break_nineteenth, 'Just_Breaks/break_nineteenth.csv')
write.csv(break_twentieth, 'Just_Breaks/break_twentieth.csv')
write.csv(break_twentyfirst, 'Just_Breaks/break_twentyfirst.csv')

# Major breaks
break_sample_major <- filter(break_sample, ROADCALL_TYPE=='MAJ')
break_sample_major <- filter(break_sample_major , DESCRIPTION!='PASSENGER STOP REQUEST')
break_sample_major$BREAK_MAJOR <- c(1)
breaks_counts <- aggregate(BREAK_MAJOR ~ VEHICLE_NUMBER, data = break_sample_major, sum)

sum(trimet_days_dep_d$BREAK)/sum(trimet_days_dep_d$MILES)*10000
sum(trimet_days_dep$BREAK)/sum(trimet_days_dep$SERVICE_DAYS)

# Visualize
break_sample_ebus <- read.csv("break_sample_ebus.csv", header = TRUE, na.strings = c("","NA"))

library(stringr)
break_sample_ebus[c('DAY', 'MYEAR')] <- str_split_fixed(break_sample_ebus$DATE, '-', 2)
break_sample_ebus$BREAK <- c(1)
break_sample_ebus_e <- filter(break_sample_ebus, EBUS==1)
break_sample_ebus_d <- filter(break_sample_ebus, EBUS!=1)

month_breaks_e <- aggregate(BREAK ~ MYEAR, data = break_sample_ebus_e, sum)
month_breaks_e$TYPE <- c('Electric')
month_breaks_d <- aggregate(BREAK ~ MYEAR, data = break_sample_ebus_d, sum)
month_breaks_d$TYPE <- c('Diesel')

month_breaks <- rbind(month_breaks_e, month_breaks_d)
month_breaks$MYEAR <- as.factor(month_breaks$MYEAR)
month_breaks$MYEAR <- factor(month_breaks$MYEAR ,levels = c("SEP-2021", "OCT-2021", "NOV-2021", "DEC-2021",
                                                            "JAN-2022", "FEB-2022","MAR-2022","APR-2022",
                                                            "MAY-2022","JUN-2022"))

ggplot(month_breaks, aes(x=MYEAR, y=BREAK, fill=TYPE)) + 
  geom_bar(stat='identity', position='dodge') +
  geom_text(aes(label=BREAK), position=position_dodge(width=0.9), vjust=-0.25) +
  xlab("Month of the Year") + ylab("# of Breaks") +
  guides(fill=guide_legend(title="Bus type")) +
  theme(legend.position = c(0.92, 0.85), text = element_text(size = 10), 
        legend.title = element_text(size = 10)) +
    scale_fill_manual("legend", values = c("Diesel" = "#F79521", "Electric" = "#0D5C91"))

ggsave("Breaks_by_month.jpg", width = 8, height = 5.5)

#trimet_breaks <- read.csv("sep 2021 vehicle failures.csv", header = TRUE, na.strings = c("","NA"))
#break_sample <- filter(trimet_breaks, ROADCALL_TYPE=='MAJ') # Only major breaks

#break_sample$DATE <- break_sample$date_event
#break_sample$DATE <- gsub("[[:punct:]]", "", break_sample$DATE)

#break_sample <- distinct(break_sample, VEHICLE_NUMBER,date_event, .keep_all= TRUE) # Remove when several breaks in one day
#break_sample <- unique(break_sample[c("VEHICLE_NUMBER", "date_event")]) # Remove when several breaks in one day

#break_btype <- merge(break_sample, trimet_bus_type, by='VEHICLE_NUMBER')

#table(break_btype$DESCRIPTION)
#test <- filter(break_btype, EBUS==1)
#write.csv(test, 'ebus_major_breaks.csv')

#names(break_sample)[names(break_sample)=='date_event'] <- 'DATE'
#break_sample$DATE <- as.Date(break_sample$DATE, format = "%d-%b-%Y")
#break_sample$VN_SD <- paste(break_sample$VEHICLE_NUMBER, break_sample$DATE,  sep = '_')


### DATA CHUNKS FOR DISTANCE BREAK MODEL =======================================
# Trimet data dictionary - http://bertini.eng.usf.edu/courses/558/dictionary.htm

# Raw data in chunks
#trimet <- read.csv('Parts/trimet.buses.2801_3101.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3102_3250.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3251_3620.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3621_3940.csv', header = TRUE, na.strings = c("","NA"))
trimet <- read.csv('Parts/trimet.buses.3941_4305.csv', header = TRUE, na.strings = c("","NA"))

#trimet <- subset(trimet, select=-c(X))
trimet$DATE <- as.Date(trimet$SERVICE_DATE, format = "%d%b%Y")

# Concatenate for unique id
trimet$SD_VN_TN_D <- paste(trimet$DATE, trimet$VEHICLE_NUMBER, trimet$TRIP_NUMBER, trimet$DIRECTION, sep = '_')

# Remove duplicate stops
trimet_single <- distinct(trimet, SD_VN_TN_D, LOCATION_ID, .keep_all= TRUE) #Remove duplicates for the same stop
rm(trimet)
gc()

# Dates in sesrvice, dates in maintenance
trimet_days <- distinct(trimet_single, VEHICLE_NUMBER, DATE) #Only unique days sample
n_distinct(trimet_days$VEHICLE_NUMBER)

# Count of gaps in service (more than a day difference) #
trimet_gaps <- trimet_days %>%
  group_by(VEHICLE_NUMBER) %>%
  arrange(DATE, .by_group = TRUE) %>%
  mutate(GAP_DAYS = 
           as.numeric(difftime(DATE, lag(DATE, default = first(DATE)), units = "days"))) # Count number of continuous service days

# All gaps (only once per bus subsample)
trimet_gaps$MAINT_N <- ifelse(trimet_gaps$GAP_DAYS>=2, 1, 0) # Count each gap longer than a day
trimet_gaps <- filter(trimet_gaps, MAINT_N!=0) # Keep only maintenance days
trimet_gaps <- data.table(trimet_gaps, key = c("VEHICLE_NUMBER", "DATE")) # Sort by vehicle number and date of maintenance
trimet_gaps_new <- trimet_gaps #Sample for the counts

# Read in breaks data 
break_1 <- read.csv('Just_Breaks/break_first.csv', colClasses=c("DATE_BREAK"="Date"))
break_2 <- read.csv('Just_Breaks/break_second.csv', colClasses=c("DATE_BREAK"="Date"))
break_3 <- read.csv('Just_Breaks/break_third.csv', colClasses=c("DATE_BREAK"="Date"))
break_4 <- read.csv('Just_Breaks/break_fourth.csv', colClasses=c("DATE_BREAK"="Date"))
break_5 <- read.csv('Just_Breaks/break_fifth.csv', colClasses=c("DATE_BREAK"="Date"))
break_6 <- read.csv('Just_Breaks/break_sixth.csv', colClasses=c("DATE_BREAK"="Date"))
break_7 <- read.csv('Just_Breaks/break_seventh.csv', colClasses=c("DATE_BREAK"="Date"))
break_8 <- read.csv('Just_Breaks/break_eighth.csv', colClasses=c("DATE_BREAK"="Date"))
break_9 <- read.csv('Just_Breaks/break_ninth.csv', colClasses=c("DATE_BREAK"="Date"))
break_10 <- read.csv('Just_Breaks/break_tenth.csv', colClasses=c("DATE_BREAK"="Date"))
break_11 <- read.csv('Just_Breaks/break_eleventh.csv', colClasses=c("DATE_BREAK"="Date"))
break_12 <- read.csv('Just_Breaks/break_twelfth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_13 <- read.csv('Just_Breaks/break_thirteenth.csv', colClasses=c("DATE_BREAK"="Date")) # From here till end - only for the first part of buses
##break_14 <- read.csv('Just_Breaks/break_fourteenth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_15 <- read.csv('Just_Breaks/break_fifteenth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_16 <- read.csv('Just_Breaks/break_sixteenth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_17 <- read.csv('Just_Breaks/break_seventeenth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_18 <- read.csv('Just_Breaks/break_eighteenth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_19 <- read.csv('Just_Breaks/break_nineteenth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_20 <- read.csv('Just_Breaks/break_twentieth.csv', colClasses=c("DATE_BREAK"="Date"))
#break_21 <- read.csv('Just_Breaks/break_twentyfirst.csv', colClasses=c("DATE_BREAK"="Date"))

# Take out maintenance and breaks before the current - date after the last break (change the break occurrence)
trimet_gaps_w <- merge(trimet_gaps, break_11, by='VEHICLE_NUMBER') # Merge with the last break
trimet_gaps_w <- filter(trimet_gaps_w, DATE>DATE_BREAK) # Filter out maintenance before the last break
trimet_gaps_new <- trimet_gaps_w # Keep separate maintenace log for maintenance counts
trimet_gaps_new <- subset(trimet_gaps_new, select=c('VEHICLE_NUMBER', 'DATE', 'GAP_DAYS', 'MAINT_N'))
trimet_gaps_w <- data.table(trimet_gaps_w, key = c("VEHICLE_NUMBER", "DATE")) # Sort by vehicle number and date of maintenance
trimet_gaps_w <- distinct(trimet_gaps_w, VEHICLE_NUMBER, .keep_all= TRUE) # The first maintenance after the last break
trimet_gaps_w <- subset(trimet_gaps_w, select=c('VEHICLE_NUMBER', 'DATE', 'GAP_DAYS', "MAINT_N"))

# Merge with breaks (change the break occurrence)
trimet_gaps_w <- merge(trimet_gaps_w, break_12, by='VEHICLE_NUMBER') # Join with break data
trimet_gaps_w <- filter(trimet_gaps_w, DATE<=DATE_BREAK) # Filter out breaks before the first maintenance
names(trimet_gaps_w)[names(trimet_gaps_w)=='DATE'] <- 'DATE_MAINT'
trimet_gaps_w <- subset(trimet_gaps_w, select=c(VEHICLE_NUMBER, DATE_MAINT, DATE_BREAK, B_OCCUR))

# First maintenance - run this part only once (Hide when not needed)
#trimet_gaps_w <- distinct(trimet_gaps, VEHICLE_NUMBER, .keep_all= TRUE) # The first maintenance
#trimet_gaps_w <- merge(trimet_gaps_w, break_1, by='VEHICLE_NUMBER') # Join with break 
#trimet_gaps_w <- filter(trimet_gaps_w, DATE<=DATE_BREAK) # Filter out breaks before the first maintenance
#names(trimet_gaps_w)[names(trimet_gaps_w)=='DATE'] <- 'DATE_MAINT'
#trimet_gaps_w <- subset(trimet_gaps_w, select=c(VEHICLE_NUMBER, DATE_MAINT, DATE_BREAK, B_OCCUR))

# Uniform for every break occurence
trimet_gaps_all <- merge(trimet_gaps_new, trimet_gaps_w, by='VEHICLE_NUMBER') # Remaining parts
trimet_gaps_all <- filter(trimet_gaps_all, DATE>=DATE_MAINT & DATE<=DATE_BREAK)
trimet_ngaps <- aggregate(MAINT_N ~ VEHICLE_NUMBER, data = trimet_gaps_all, sum) # Aggregate maintenance #

trimet_days_w <- merge(trimet_days, trimet_gaps_w, by='VEHICLE_NUMBER') # Merge with all operations days
trimet_days_break <- filter(trimet_days_w, DATE>=DATE_MAINT & DATE<=DATE_BREAK) # Keep only operations between maintenance and break

# Calculate number of service days
trimet_days_service <- trimet_days_break %>% count(VEHICLE_NUMBER) # Count number of service days
names(trimet_days_service)[names(trimet_days_service)=='n'] <- 'SERVICE_DAYS' 

# Add maintenance #
trimet_days_service <- merge(trimet_days_service, trimet_ngaps, by='VEHICLE_NUMBER', all.x=TRUE)
trimet_days_service[is.na(trimet_days_service)] <- 0 # Replace NAs for no maintenance with 0

# Operations only in the period between the maintenance and break
trimet_single_w <- merge(trimet_single, trimet_gaps_w, by='VEHICLE_NUMBER')
trimet_single_w <- filter(trimet_single_w, DATE>=DATE_MAINT & DATE<=DATE_BREAK)

# Add e-bus
trimet_bus_type <- read.csv("Variables/trimet_bus_type.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_type <- subset(trimet_bus_type, select=-c(X))
trimet_days_dep <- merge(trimet_days_service, trimet_bus_type, by='VEHICLE_NUMBER') 

# Add # of actual stops
trimet_single_w$A_STOP <- ifelse(trimet_single_w$DWELL>=1, 1, 0) #If DWELL was more than 0 (Door opened)
trimet_actual_stops <- aggregate(A_STOP ~ VEHICLE_NUMBER, data = trimet_single_w, sum)
trimet_days_dep <- merge(trimet_days_dep, trimet_actual_stops, by='VEHICLE_NUMBER')

# Add average load
#trimet_single <- filter(trimet_single, ESTIMATED_LOAD>=0) # Only for the mistake in the first chunk
trimet_avg_load <- aggregate(ESTIMATED_LOAD ~ VEHICLE_NUMBER, data = trimet_single_w, mean)
names(trimet_avg_load)[names(trimet_avg_load)=='ESTIMATED_LOAD'] <- 'AVG_LOAD'
trimet_days_dep <- merge(trimet_days_dep, trimet_avg_load, by='VEHICLE_NUMBER')

# Add average experience
trimet_exp <- aggregate(operator_seniority ~ VEHICLE_NUMBER, data = trimet_single_w, mean)
names(trimet_exp)[names(trimet_exp)=='operator_seniority'] <- 'OP_EXP'
trimet_days_dep <- merge(trimet_days_dep, trimet_exp, by='VEHICLE_NUMBER')

# Add # of times lift was used
trimet_lift <- aggregate(LIFT ~ VEHICLE_NUMBER, data = trimet_single_w, sum)
trimet_days_dep <- merge(trimet_days_dep, trimet_lift, by='VEHICLE_NUMBER')

# Add average max speed
trimet_max_speed <- aggregate(MAXIMUM_SPEED ~ VEHICLE_NUMBER, data = trimet_single_w, mean)
trimet_days_dep <- merge(trimet_days_dep, trimet_max_speed, by='VEHICLE_NUMBER')

# Add planned odometer reading miles for the period
trimet_odometer <- read.csv("bus_miles_sept_2021.csv", header = TRUE, na.strings = c("","NA"))
names(trimet_odometer)[names(trimet_odometer)=='EQUIPMENT_NUMBER'] <-'VEHICLE_NUMBER'
trimet_odometer <- merge(trimet_odometer, trimet_gaps_w, by='VEHICLE_NUMBER')
trimet_odometer$DATE <- as.Date(trimet_odometer$POST_DATE, format = "%d%b%Y")
trimet_odometer <- filter(trimet_odometer, DATE>=DATE_MAINT & DATE<=DATE_BREAK)

trimet_odm <- aggregate(MILES ~ VEHICLE_NUMBER, data = trimet_odometer, sum)
trimet_days_dep <- merge(trimet_days_dep, trimet_odm, by='VEHICLE_NUMBER')
trimet_days_dep <- merge(trimet_days_dep, trimet_gaps_w, by='VEHICLE_NUMBER')

# Create a new df for the specific occurence of a break
#trimet_days_dep_p1 <- trimet_days_dep
#trimet_days_dep_p2 <- trimet_days_dep
#trimet_days_dep_p3 <- trimet_days_dep
#trimet_days_dep_p4 <- trimet_days_dep
#trimet_days_dep_p5 <- trimet_days_dep
#trimet_days_dep_p6 <- trimet_days_dep
#trimet_days_dep_p7 <- trimet_days_dep
#trimet_days_dep_p8 <- trimet_days_dep
#trimet_days_dep_p9 <- trimet_days_dep
#trimet_days_dep_p10 <- trimet_days_dep
#trimet_days_dep_p11 <- trimet_days_dep
trimet_days_dep_p12 <- trimet_days_dep
#trimet_days_dep_p13 <- trimet_days_dep # From here till end - only for the first part of buses
#trimet_days_dep_p14 <- trimet_days_dep
#trimet_days_dep_p15 <- trimet_days_dep
#trimet_days_dep_p16 <- trimet_days_dep #No matches
#trimet_days_dep_p17 <- trimet_days_dep
#trimet_days_dep_p18 <- trimet_days_dep #No matches
#trimet_days_dep_p19 <- trimet_days_dep
#trimet_days_dep_p20 <- trimet_days_dep
#trimet_days_dep_p21 <- trimet_days_dep

# clean memory (internal)
rm(list= ls()[!(ls() %in% c('trimet_single', 'trimet_days','trimet_gaps', 
                            'trimet_days_dep_p1', 'trimet_days_dep_p2', 'trimet_days_dep_p3',
                            'trimet_days_dep_p4', 'trimet_days_dep_p5', 'trimet_days_dep_p6',
                            'trimet_days_dep_p7', 'trimet_days_dep_p8', 'trimet_days_dep_p9',
                            'trimet_days_dep_p10', 'trimet_days_dep_p11', 'trimet_days_dep_p12',
                            'trimet_days_dep_p13', 'trimet_days_dep_p14', 'trimet_days_dep_p15',
                            'trimet_days_dep_p17', 'trimet_days_dep_p19', 'trimet_days_dep_p20',
                            'trimet_days_dep_p21'))])
gc()

# Join all breaks for a sample of buses
trimet_days_dep <- rbind(trimet_days_dep_p1, trimet_days_dep_p2, trimet_days_dep_p3,
                         trimet_days_dep_p4, trimet_days_dep_p5, trimet_days_dep_p6,
                         trimet_days_dep_p7, trimet_days_dep_p8, trimet_days_dep_p9,
                         trimet_days_dep_p10, trimet_days_dep_p11, trimet_days_dep_p12)

#                        trimet_days_dep_p13, trimet_days_dep_p14, trimet_days_dep_p15,
#                        trimet_days_dep_p17, trimet_days_dep_p19, trimet_days_dep_p20,
#                        trimet_days_dep_p21)

#write.csv(trimet_days_dep, 'All_Break_Parts/trimet_days_allbreak.2801_3101.csv')
#write.csv(trimet_days_dep, 'All_Break_Parts/trimet_days_allbreak.3102_3250.csv')
#write.csv(trimet_days_dep, 'All_Break_Parts/trimet_days_allbreak.3251_3620.csv')
#write.csv(trimet_days_dep, 'All_Break_Parts/trimet_days_allbreak.3621_3940.csv')
write.csv(trimet_days_dep, 'All_Break_Parts/trimet_days_allbreak.3941_4305.csv')

rm(list = ls())
gc()

trimet_days_dep_1 <- read.csv('All_Break_Parts/trimet_days_allbreak.2801_3101.csv')
trimet_days_dep_2 <- read.csv('All_Break_Parts/trimet_days_allbreak.3102_3250.csv')
trimet_days_dep_3 <- read.csv('All_Break_Parts/trimet_days_allbreak.3251_3620.csv')
trimet_days_dep_4 <- read.csv('All_Break_Parts/trimet_days_allbreak.3621_3940.csv')
trimet_days_dep_5 <- read.csv('All_Break_Parts/trimet_days_allbreak.3941_4305.csv')

trimet_days_dep <- rbind(trimet_days_dep_1, trimet_days_dep_2, trimet_days_dep_3,
                         trimet_days_dep_4, trimet_days_dep_5)

trimet_days_dep <- subset(trimet_days_dep, select=-c(X))
trimet_days_dep$DATE_BREAK <- as.Date(trimet_days_dep$DATE_BREAK, format = "%Y-%m-%d")

# Add weather
trimet_weather <- read.csv("trimet_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_weather$DATE <- as.Date(trimet_weather$DATE, format = "%d-%b-%y")


trimet_days_dep <- merge(trimet_days_dep, trimet_weather, by.x='DATE_BREAK', by.y='DATE')


write.csv(trimet_days_dep, 'trimet_break_all.csv')

### DISTANCE BREAK MULTILEVEL MODEL ============================================
library(lme4)
library(lattice)
library(MuMIn) #http://mindingthebrain.blogspot.com/2014/02/three-ways-to-get-parameter-specific-p.html
library(lmerTest)
library(sjPlot)
#library(multilevelTools)

trimet_break_all <- read.csv('trimet_break_all.csv')
trimet_break_all$LIFT_DWELL <- trimet_break_all$LIFT/trimet_break_all$A_STOP*100
trimet_break_all$A_STOP <- trimet_break_all$A_STOP/100
trimet_break_all$A_STOP2 <- trimet_break_all$A_STOP^2
trimet_break_all$OP_EXP2 <- trimet_break_all$OP_EXP^2
trimet_break_all$B_OCCURS <- trimet_break_all$B_OCCUR-1
trimet_break_all$DATE_MAINT <- as.Date(trimet_break_all$DATE_MAINT, format = "%Y-%m-%d")

trimet_year <- read.csv('Variables/trimet_bus_year_model.csv')
trimet_break_all <- merge(trimet_break_all, trimet_year, by='VEHICLE_NUMBER')

hybrid_break <- filter(trimet_break_all, HYBRID==1)
mean(trimet_break_all$HYBRID)
sd(trimet_break_all$HYBRID)

# Welch's T-test
miles_diesel <- trimet_break_all %>% filter(EBUS==0) %>% subset(select=c(MILES)) 
miles_diesel <- miles_diesel[['MILES']]
miles_hybrid <- trimet_break_all %>% filter(HYBRID==1) %>% subset(select=c(MILES)) 
miles_hybrid <- miles_hybrid[['MILES']]
miles_ebus <- trimet_break_all %>% filter(EBUS==1) %>% subset(select=c(MILES)) 
miles_ebus <- miles_ebus[['MILES']]

t.test(miles_diesel, miles_ebus, var.equal = F)
t.test(miles_diesel, miles_hybrid, var.equal = F)

main_diesel <- trimet_break_all %>% filter(EBUS==0) %>% subset(select=c(MAINT_N)) 
main_diesel <- main_diesel[['MAINT_N']]
main_ebus <- trimet_break_all %>% filter(EBUS==1) %>% subset(select=c(MAINT_N)) 
main_ebus <- main_ebus[['MAINT_N']]
main_hybrid <- trimet_break_all %>% filter(HYBRID==1) %>% subset(select=c(MAINT_N)) 
main_hybrid <- miles_hybrid[['MAINT_N']]

t.test(main_diesel, main_hybrid, var.equal = F)

dwell_diesel <- trimet_break_all %>% filter(EBUS==0) %>% subset(select=c(A_STOP)) 
dwell_diesel <- dwell_diesel[['A_STOP']]
dwell_ebus <- trimet_break_all %>% filter(EBUS==1) %>% subset(select=c(A_STOP)) 
dwell_ebus <- dwell_ebus[['A_STOP']]
t.test(dwell_diesel, dwell_ebus, var.equal = F)

trimet_break_all <- data.table(trimet_break_all, key = c("VEHICLE_NUMBER", "DATE_MAINT"))

# Unconditional means model
m_null<-lmer(data = trimet_break_all, MILES ~ 1 + (1 | VEHICLE_NUMBER), REML=F)
summary(m_null)

# Intra-class correlation coefficient
(1185^2)/((1185^2)+(4455^2)) # 0.066 (more than 0.05) clustering exists

xyplot(MILES ~ MAINT_N | VEHICLE_NUMBER, data=trimet_break_all, type=c('p','r'))

# Unconditional growth model (Random slope for ebus)

break_distance<-lmer(data = trimet_break_comb, MILES ~ 1 + B_OCCURS + MAINT_N + AVG_LOAD +  
                       A_STOP*EBUS + A_STOP2 + LIFT_DWELL*EBUS + HYBRID + AVG_TEMP + 
                (1 + EBUS | VEHICLE_NUMBER), REML=F)
summary(break_distance)
r.squaredGLMM(break_distance)
confint(break_distance, level = 0.9)
icc(break_distance)

plot(break_distance)

trimet_break_all$BTYPE <- ifelse(trimet_break_all$EBUS==1, 1,
                                 trimet_break_all$HYBRID==1, 2,
                                 3)

res <- bartlett.test(MILES ~ EBUS, data = trimet_break_all)

#modelDiagnostics(break_distance, ev.perc = .1)

trimet_break_d <- sample_n(trimet_break_clean, 500)
trimet_break_d <- trimet_break_d %>% group_by(VEHICLE_NUMBER) %>% filter( n() > 1 )

trimet_break_comb <- rbind(trimet_break_e, trimet_break_h, trimet_break_d)

# For experimenting
break_distance<-lmer(data = trimet_break_all, MILES ~ B_OCCURS + AVG_TEMP + MAINT_N + AVG_LOAD + A_STOP + A_STOP2 + LIFT_DWELL*EBUS +
                       (EBUS | VEHICLE_NUMBER), REML=F)
summary(m_fixed)

### DISTANCE BREAK MODEL OLD ===================================================
library(car)
library(lmtest)
library(sandwich)
library(stargazer)

trimet_days_break <- read.csv('trimet_break_first.csv')
trimet_days_break$LIFT_DWELL <- trimet_days_break$LIFT/trimet_days_break$A_STOP*100
trimet_days_break$A_STOP <- trimet_days_break$A_STOP/1000

#trimet_days_break$DATE_BREAK <- as.Date(trimet_days_break$DATE_BREAK, format = "%Y-%m-%d")
#trimet_days_break$DATE_MAINT <- as.Date(trimet_days_break$DATE_MAINT, format = "%Y-%m-%d")
#trimet_days_break <- trimet_days_break %>% mutate(CALENDAR_DAYS = as.numeric(difftime(DATE_BREAK, DATE_MAINT, units = "days")))

#trimet_days_break$OFFSERVICE <- trimet_days_break$CALENDAR_DAYS-trimet_days_break$SERVICE_DAYS+1
#trimet_days_break$OFFSERVICE_PERC <- trimet_days_break$OFFSERVICE/trimet_days_break$CALENDAR_DAYS*100
#trimet_days_break[is.na(trimet_days_break)] <- 0

# Check distribution
hist(trimet_days_break$MAXIMUM_SPEED)

# OLS
model_distance_breaks <- lm(MILES ~ MAINT_N + LIFT_DWELL*EBUS + A_STOP + I(A_STOP^2) +  MAXIMUM_SPEED, 
                   data = trimet_days_break)
summary(model_distance_breaks)

AVG_TEMP <- c(50.56) #(54.93)
LIFT_DWELL <- c(2.07) #(2.173)
A_STOP <- c(7.64587) #(6.024)
AVG_LOAD <- c(5.445)
OP_EXP <- c(8.39) #(8.376)
MAXIMUM_SPEED <- c(26.49) #(26.59)
EBUS <- c(0)

trimet_bus <- data.frame(AVG_TEMP, LIFT_DWELL, A_STOP, AVG_LOAD, OP_EXP, MAXIMUM_SPEED, EBUS)
trimet_ebus <- data.frame(AVG_TEMP, LIFT_DWELL, A_STOP, OP_EXP, MAXIMUM_SPEED, EBUS)

predict(model_distance_breaks, newdata = trimet_bus)
predict(model_distance_breaks, newdata = trimet_ebus)

#summary(gvmodel)
library(lmtest)
bptest(model_distance_breaks) #studentized Breusch-Pagan test

par(mfrow = c(2, 2))
plot(model_breaks)

library(car)
vif(model_distance_breaks)

# GLM

logit <- glm(formula = EBUS ~ MILES + AVG_TEMP + MAXIMUM_SPEED, data = trimet_days_break)
summary(logit)

logit0<-update(logit, formula= MILES ~ 1)
McFadden<- 1-as.vector(logLik(logit)/logLik(logit0))
McFadden


### DATA CHUNKS FOR BREAK MODEL ================================================
# Trimet data dictionary - http://bertini.eng.usf.edu/courses/558/dictionary.htm
breaks_counts <- read.csv("break_counts_all_clean.csv", header = TRUE, na.strings = c("","NA"))
breaks_for_sample <- read.csv("break_all_clean.csv", header = TRUE, na.strings = c("","NA"))
breaks_for_sample <- subset(breaks_for_sample, select=c(SD_VN, BREAK))

# Raw data in chunks
#trimet <- read.csv('Parts/trimet.buses.2801_3101.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3102_3250.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3251_3620.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3621_3940.csv', header = TRUE, na.strings = c("","NA"))
trimet <- read.csv('Parts/trimet.buses.3941_4305.csv', header = TRUE, na.strings = c("","NA"))

#trimet <- subset(trimet, select=-c(X))
trimet$DATE <- as.Date(trimet$SERVICE_DATE, format = "%d%b%Y")

# Concatenate for unique id
trimet$SD_VN_TN_D <- paste(trimet$DATE, trimet$VEHICLE_NUMBER, trimet$TRIP_NUMBER, trimet$DIRECTION, sep = '_')

# Remove duplicate stops
trimet_single <- distinct(trimet, SD_VN_TN_D, LOCATION_ID, .keep_all= TRUE) #Remove duplicates for the same stop
rm(trimet)
gc()

# Concatenate for unique vehicle id-day
#trimet_single$SD_VN <- paste(trimet_single$DATE, trimet_single$VEHICLE_NUMBER, sep = '_')
#trimet_single_nfl <- filter(trimet_single, SCHEDULE_STATUS<=4) #Sample without first and last stops
#trimet_single$DATE <- as.Date(trimet_single$SERVICE_DATE, format = "%d%b%Y")

# Dates in sesrvice, dates in maintenance
trimet_days <- distinct(trimet_single, VEHICLE_NUMBER, DATE) #Only unique days sample

# Count of gaps in service (more than a day difference)
trimet_gaps <- trimet_days %>%
  group_by(VEHICLE_NUMBER) %>%
  arrange(DATE, .by_group = TRUE) %>%
  mutate(GAP_DAYS = 
           as.numeric(difftime(DATE, lag(DATE, default = first(DATE)), units = "days"))) # Count number of continuous service days

trimet_gaps$SD_VN <- paste(trimet_gaps$DATE, trimet_gaps$VEHICLE_NUMBER, sep = '_')
trimet_gaps <- merge(trimet_gaps, breaks_for_sample, by='SD_VN', all.x=TRUE) # Merge with date-vehicle breaks
trimet_gaps <- trimet_gaps %>% filter(is.na(BREAK)) # Remove counts with breaks

trimet_gaps$MAINT_N <- ifelse(trimet_gaps$GAP_DAYS>=2, 1, 0) # Count each gap longer than a day
trimet_ngaps <- aggregate(MAINT_N ~ VEHICLE_NUMBER, data = trimet_gaps, sum) 

# Calculate number of total and service days
trimet_days_service <- trimet_days %>% count(VEHICLE_NUMBER)
names(trimet_days_service)[names(trimet_days_service)=='n'] <- 'SERVICE_DAYS' # Count number of service days

trimet_days <- trimet_days %>% arrange(VEHICLE_NUMBER,DATE) #Sort by date ascending
trimet_days_first <- trimet_days[!duplicated(trimet_days$VEHICLE_NUMBER),]
names(trimet_days_first)[names(trimet_days_first)=='DATE'] <- 'DATE_FIRST'
trimet_days <- trimet_days %>% arrange(VEHICLE_NUMBER, desc(DATE)) #Sort by date descending
trimet_days_last <- trimet_days[!duplicated(trimet_days$VEHICLE_NUMBER),]
names(trimet_days_last)[names(trimet_days_last)=='DATE'] <- 'DATE_LAST'

trimet_days_firstlast <- merge(trimet_days_first, trimet_days_last, by='VEHICLE_NUMBER')
trimet_days_firstlast <- trimet_days_firstlast %>% mutate(CALENDAR_DAYS = 
                         as.numeric(difftime(DATE_LAST, DATE_FIRST, units = "days")))

# Merge days and breaks and all that jazz
trimet_days_dep <- merge(trimet_days_service, trimet_days_firstlast, by='VEHICLE_NUMBER')
trimet_days_dep$MAINT_DAYS <- trimet_days_dep$CALENDAR_DAYS - trimet_days_dep$SERVICE_DAYS # Days spent in maintenance

trimet_days_dep <- merge(trimet_days_dep, trimet_ngaps, by='VEHICLE_NUMBER') # Add maintenance counts
trimet_days_dep <- merge(trimet_days_dep, breaks_counts, by='VEHICLE_NUMBER', all.x=TRUE) # Add breaks

trimet_days_dep$BREAK_PERC <- trimet_days_dep$BREAK/trimet_days_dep$SERVICE_DAYS*100 # Dependent variable

# Add e-bus
trimet_bus_type <- read.csv("Variables/trimet_bus_type.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_type <- subset(trimet_bus_type, select=-c(X))
trimet_days_dep <- merge(trimet_days_dep, trimet_bus_type, by='VEHICLE_NUMBER') 

# Add # of actual stops
trimet_single$A_STOP <- ifelse(trimet_single$DWELL>=1, 1, 0) #If DWELL was more than 0 (Door opened)
trimet_actual_stops <- aggregate(A_STOP ~ VEHICLE_NUMBER, data = trimet_single, sum)
trimet_days_dep <- merge(trimet_days_dep, trimet_actual_stops, by='VEHICLE_NUMBER')

# Add average load
#trimet_single <- filter(trimet_single, ESTIMATED_LOAD>=0) # Only for the mistake in the first chunk
trimet_avg_load <- aggregate(ESTIMATED_LOAD ~ VEHICLE_NUMBER, data = trimet_single, mean)
names(trimet_avg_load)[names(trimet_avg_load)=='ESTIMATED_LOAD'] <- 'AVG_LOAD'
trimet_days_dep <- merge(trimet_days_dep, trimet_avg_load, by='VEHICLE_NUMBER')

# Add average experience
trimet_exp <- aggregate(operator_seniority ~ VEHICLE_NUMBER, data = trimet_single, mean)
names(trimet_exp)[names(trimet_exp)=='operator_seniority'] <- 'OP_EXP'
trimet_days_dep <- merge(trimet_days_dep, trimet_exp, by='VEHICLE_NUMBER')

# Add # of times lift was used
trimet_lift <- aggregate(LIFT ~ VEHICLE_NUMBER, data = trimet_single, sum)
trimet_days_dep <- merge(trimet_days_dep, trimet_lift, by='VEHICLE_NUMBER')

# Add average max speed
trimet_max_speed <- aggregate(MAXIMUM_SPEED ~ VEHICLE_NUMBER, data = trimet_single, mean)
trimet_days_dep <- merge(trimet_days_dep, trimet_max_speed, by='VEHICLE_NUMBER')

# Add planned odometer reading miles
trimet_odometer <- read.csv("bus_miles_sept_2021.csv", header = TRUE, na.strings = c("","NA"))
trimet_odm <- aggregate(MILES ~ EQUIPMENT_NUMBER, data = trimet_odometer, sum)
names(trimet_odm)[names(trimet_odm)=='EQUIPMENT_NUMBER'] <- 'VEHICLE_NUMBER'
trimet_days_dep <- merge(trimet_days_dep, trimet_odm, by='VEHICLE_NUMBER')

trimet_days_dep[is.na(trimet_days_dep)] <- 0 # Replace NAs for no breaks with 0

#write.csv(trimet_days_dep, 'Break_Parts/trimet_days_dep.2801_3101.csv')
#write.csv(trimet_days_dep, 'Break_Parts/trimet_days_dep.3102_3250.csv')
#write.csv(trimet_days_dep, 'Break_Parts/trimet_days_dep.3251_3620.csv')
#write.csv(trimet_days_dep, 'Break_Parts/trimet_days_dep.3621_3940.csv')
write.csv(trimet_days_dep, 'Break_Parts/trimet_days_dep.3941_4305.csv')

rm(list = ls())
gc()

trimet_days_dep_1 <- read.csv('Break_Parts/trimet_days_dep.2801_3101.csv')
trimet_days_dep_2 <- read.csv('Break_Parts/trimet_days_dep.3102_3250.csv')
trimet_days_dep_3 <- read.csv('Break_Parts/trimet_days_dep.3251_3620.csv')
trimet_days_dep_4 <- read.csv('Break_Parts/trimet_days_dep.3621_3940.csv')
trimet_days_dep_5 <- read.csv('Break_Parts/trimet_days_dep.3941_4305.csv')

trimet_days_dep <- rbind(trimet_days_dep_1, trimet_days_dep_2, trimet_days_dep_3,
                         trimet_days_dep_4, trimet_days_dep_5)

trimet_days_dep <- subset(trimet_days_dep, select=-c(X, X.1))

# Add weather
trimet_weather <- read.csv("trimet_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_weather$DATE <- as.Date(trimet_weather$DATE, format = "%d-%b-%y")
breaks_for_sample <- read.csv("break_all_clean.csv", header = TRUE, na.strings = c("","NA"))
breaks_for_sample$DATE <- as.Date(breaks_for_sample$DATE, format = "%Y-%m-%d")

trimet_break_weather <- merge(breaks_for_sample, trimet_weather, by='DATE')
breaks_temp <- aggregate(AVG_TEMP ~ VEHICLE_NUMBER, data = trimet_break_weather, mean)
breaks_precip <- aggregate(PRECIP ~ VEHICLE_NUMBER, data = trimet_break_weather, mean)

breaks_weather <- merge(breaks_temp, breaks_precip, by='VEHICLE_NUMBER')
trimet_days_dep<- merge(trimet_days_dep, breaks_weather, by='VEHICLE_NUMBER', all.x=TRUE)

trimet_days_dep$AVG_TEMP[is.na(trimet_days_dep$AVG_TEMP)] <- 50.56
trimet_days_dep$PRECIP[is.na(trimet_days_dep$PRECIP)] <- 0.1533

write.csv(trimet_days_dep, 'trimet_days_dep.csv')

### PERCENTAGE BREAKS MODEL ====================================================
library(car)
library(lmtest)
library(sandwich)
library(stargazer)

trimet_days_dep <- read.csv('trimet_days_dep.csv')
trimet_days_dep <- subset(trimet_days_dep, select=-c(X))
trimet_days_dep$MAINT_MILES <- trimet_days_dep$MAINT_N/trimet_days_dep$MILES*1000
trimet_days_dep$LIFT_DWELL <- trimet_days_dep$LIFT/trimet_days_dep$A_STOP*100
trimet_days_dep$OFFSERVICE_PERC <- trimet_days_dep$MAINT_DAYS/trimet_days_dep$CALENDAR_DAYS*100
trimet_days_dep$MILES <- trimet_days_dep$MILES/10000
trimet_days_dep$A_STOP <- trimet_days_dep$A_STOP/1000

#trimet_days_dep$DWELL_MILES <- trimet_days_dep$A_STOP/trimet_days_dep$MILES
#trimet_days_dep$BREAK_MILES <- trimet_days_dep$BREAK/trimet_days_dep$MILES*10000
#trimet_days_dep$BREAK_RATE <- trimet_days_dep$BREAK_PERC/100

#trimet_days_dep_c <- filter(trimet_days_dep, BREAK_PERC<=12.35)
#trimet_days_dep_nz <- filter(trimet_days_dep, BREAK_PERC>=0.1)

#trimet_days_dep_maj <- merge(trimet_days_dep_c, breaks_counts, by='VEHICLE_NUMBER', all.x = TRUE)
#trimet_days_dep_maj[is.na(trimet_days_dep_maj)] <- 0 # Replace NAs for no breaks with 0
#trimet_days_dep_maj$BREAK_PERC_MAJOR <- trimet_days_dep_maj$BREAK_MAJOR/trimet_days_dep_maj$SERVICE_DAYS*100

# Check distribution
hist(trimet_days_dep_nz$BREAK_PERC_BC)

# OLS
model_breaks <- lm(BREAK_PERC ~ AVG_TEMP + MAINT_MILES + OFFSERVICE_PERC + 
                   LIFT_DWELL + AVG_LOAD + A_STOP + OP_EXP + I(OP_EXP^2) + MAXIMUM_SPEED + MILES*EBUS, 
                   data = trimet_days_dep)
summary(model_breaks)

#summary(gvmodel)
bptest(model_breaks) #studentized Breusch-Pagan test

par(mfrow = c(2, 2))
plot(model_breaks)

vif(model_breaks)

# Dealing with heteroskedasticity https://rpubs.com/cyobero/187387

# Robust SE
stargazer(model_breaks,
          coeftest(model_breaks, vcovHC(model_breaks, type = "HC0")),
          coeftest(model_breaks, vcovHC(model_breaks, type = "HC1")),
          coeftest(model_breaks, vcovHC(model_breaks, type = "HC2")),
          coeftest(model_breaks, vcovHC(model_breaks, type = "HC3")),
          type = "text")

stargazer(model_breaks, coeftest(model_breaks, vcovHC(model_breaks, type = "HC1")), type = "text", out='filename.tex')

# Weighting 
model_breaks.ols <- lm(BREAK_PERC ~ SERVICE_DAYS + MAINT_MILES + I(MAINT_MILES^2) +
                         LIFT_DWELL + AVG_LOAD + I(AVG_LOAD^2) + A_STOP + I(A_STOP^2) + OP_EXP + I(OP_EXP^2) + MILES*EBUS, 
                       data = trimet_days_dep)
trimet_days_dep$resi <- model_breaks.ols$residuals
varfunc.ols <- lm(log(resi^2) ~ log(SERVICE_DAYS) + log(MAINT_MILES) + log(I(MAINT_MILES^2)) + log(AVG_LOAD) + 
                    log(A_STOP) + log(OP_EXP) + log(LIFT_DWELL+1) + log(MILES*EBUS), data = trimet_days_dep)
trimet_days_dep$varfunc <- exp(varfunc.ols$fitted.values)
trimet_days_dep.gls <- lm(BREAK_PERC ~ SERVICE_DAYS + MAINT_MILES + I(MAINT_MILES^2) +
                            LIFT_DWELL + AVG_LOAD + I(AVG_LOAD^2) + A_STOP + I(A_STOP^2) + 
                            OP_EXP + I(OP_EXP^2) + MILES*EBUS, weights = 1/sqrt(varfunc),
                          data = trimet_days_dep)
summary(trimet_days_dep.gls)



# Box Cox Transformation
library(caret)
bc_break <- BoxCoxTrans(trimet_days_dep$BREAK_PERC)
trimet_days_dep <- cbind(trimet_days_dep, BREAK_PERC_BC = predict(BC_BREAK_PERC, trimet_days_dep$BREAK_PERC))


model_breaks_t <- lm(log(BREAK_RATE) ~ SERVICE_DAYS + MAINT_MILES + 
                     LIFT_DWELL + AVG_LOAD + log(A_STOP)*EBUS + OP_EXP + MILES,
                   data = trimet_days_dep_nz)
summary(model_breaks_t)

library(gvlma)
gvmodel <- gvlma(model_breaks)
summary(gvmodel)

#summary(gvmodel)
bptest(model_breaks) #studentized Breusch-Pagan test

par(mfrow = c(2, 2))
plot(model_breaks)

vif(model_breaks)

### PERCENTAGE BREKS SUMMARY STATS =============================================
library(pastecs)

trimet_days_dep_e <- filter(trimet_days_dep, EBUS==1)
trimet_days_dep_d <- filter(trimet_days_dep, EBUS!=1)

stat.desc(trimet_days_dep)
stat.desc(trimet_days_dep_e)
stat.desc(trimet_days_dep_d)

summary(trimet_days_dep$BREAK_PERC)
sd(trimet_days_dep$BREAK_PERC)

summary(trimet_days_dep_e$OP_EXP)
sd_e(trimet_days_dep$MILES)

### DATA CHUNKS FOR RUNTIME MODEL ==============================================
# Raw data in chunks
#trimet <- read.csv('Parts/trimet.buses.2801_3101.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3102_3250.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3251_3620.csv', header = TRUE, na.strings = c("","NA"))
#trimet <- read.csv('Parts/trimet.buses.3621_3940.csv', header = TRUE, na.strings = c("","NA"))
trimet <- read.csv('Parts/trimet.buses.3941_4305.csv', header = TRUE, na.strings = c("","NA"))

#trimet <- subset(trimet, select=-c(X))
Routes <- c(6,8,20,62) # Limit to buses with electric routes only
trimet <- filter(trimet, ROUTE_NUMBER %in% Routes)
trimet$DATE <- as.Date(trimet$SERVICE_DATE, format = "%d%b%Y")

# Concatenate for unique id
trimet$SD_VN_TN_D <- paste(trimet$DATE, trimet$VEHICLE_NUMBER, trimet$TRIP_NUMBER, trimet$DIRECTION, sep = '_')

# Remove duplicate stops
trimet_single <- distinct(trimet, SD_VN_TN_D, LOCATION_ID, .keep_all= TRUE) #Remove duplicates for the same stop
trimet_single_nfl <- filter(trimet_single, SCHEDULE_STATUS<=4)
rm(trimet)
gc()

# Runtime
trimet_start <- filter(trimet_single, SCHEDULE_STATUS==5) #First Stop
trimet_start <- subset(trimet_start, select=c(SD_VN_TN_D, LEAVE_TIME, ROUTE_NUMBER))

trimet_finish <- filter(trimet_single, SCHEDULE_STATUS==6) #Last Stop
trimet_finish <- subset(trimet_finish, select=c(SD_VN_TN_D, ARRIVE_TIME))

trimet_runtime <-merge(trimet_start, trimet_finish, by='SD_VN_TN_D')
trimet_runtime <- mutate(trimet_runtime, RUNTIME=ARRIVE_TIME-LEAVE_TIME)
rm(trimet_start, trimet_finish)

# Number of stops
trimet_single$A_STOP <- ifelse(trimet_single$DWELL>=1, 1, 0) #If DWELL was more than 0 (Door opened)
trimet_actual_stops <- aggregate(A_STOP ~ SD_VN_TN_D, data = trimet_single, sum)

# Total Ons and Offs
trimet_ons <- aggregate(ONS ~ SD_VN_TN_D, data = trimet_single_nfl, sum)
trimet_offs <- aggregate(OFFS ~ SD_VN_TN_D, data = trimet_single_nfl, sum)
trimet_ons_offs <- merge(trimet_ons, trimet_offs, by='SD_VN_TN_D') #Note: 0.97 correlation
trimet_ons_offs$PACKS <- trimet_ons_offs$ONS + trimet_ons_offs$OFFS
rm(trimet_ons, trimet_offs)

# Average load
trimet_avg_load <- aggregate(ESTIMATED_LOAD ~ SD_VN_TN_D, data = trimet_single_nfl, mean)
names(trimet_avg_load)[names(trimet_avg_load)=='ESTIMATED_LOAD'] <- 'AVG_LOAD'

# Time of day
trimet_time <- filter(trimet_single, SCHEDULE_STATUS==5) #First Stop
trimet_time <- subset(trimet_time, select=c(SD_VN_TN_D, LEAVE_TIME))
trimet_time$START_H <- trimet_time$LEAVE_TIME/3600

trimet_time$TIME_LEVELS <-ifelse(trimet_time$START_H<=6.50, 1, 
                                 ifelse(trimet_time$START_H>6.50 & trimet_time$START_H<=9.50 , 2, 
                                        ifelse(trimet_time$START_H>9.50 & trimet_time$START_H<=15.50 , 3, 
                                               ifelse(trimet_time$START_H>15.50 & trimet_time$START_H<=18.50 , 4, 
                                                      0))))
trimet_time <- subset(trimet_time, select=-c(START_H,LEAVE_TIME))

# Route, Direction, Service, Experience
trimet_rdle <- filter(trimet_single, SCHEDULE_STATUS==5) #First Stop
trimet_rdle <- subset(trimet_rdle, select=c(SD_VN_TN_D, ROUTE_NUMBER, DIRECTION, SERVICE_KEY, operator_seniority, propulsion))

trimet_rdle$ROUTE_NUMBER <- as.factor(trimet_rdle$ROUTE_NUMBER)
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='20'] <- '0'
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='6'] <- '1'
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='8'] <- '2'
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='62'] <- '3'
names(trimet_rdle)[names(trimet_rdle)=='ROUTE_NUMBER'] <- 'ROUTE'

trimet_rdle$SCH_WEEK <- ifelse(trimet_rdle$SERVICE_KEY=='W', 1, 0)

names(trimet_rdle)[names(trimet_rdle)=='DIRECTION'] <- 'INBOUND'
names(trimet_rdle)[names(trimet_rdle)=='operator_seniority'] <- 'OPERATOR_EXP'
trimet_rdle$EBUS <- ifelse(trimet_rdle$propulsion=='E', 1, 0)
trimet_rdle <- subset(trimet_rdle, select=-c(SERVICE_KEY, propulsion))

# Lift
trimet_lift <- aggregate(LIFT ~ SD_VN_TN_D, data = trimet_single, sum)

# Merge all
trimet_runtime_model <- merge(trimet_runtime, trimet_avg_load, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_lift, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_ons_offs, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_rdle, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_time, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_actual_stops, by='SD_VN_TN_D')

rm(trimet_runtime, trimet_avg_load, trimet_lift, trimet_ons_offs, trimet_rdle, 
   trimet_time, trimet_actual_stops)

# Route 6
trimet_runtime_model_6 <- filter(trimet_runtime_model, ROUTE_NUMBER==6)
sd(trimet_runtime_model_6$A_STOP)*3 #17.20; 
summary(trimet_runtime_model_6$A_STOP)

trimet_runtime_model_6c <- filter(trimet_runtime_model_6, A_STOP>=5)
trimet_runtime_model_6c <- filter(trimet_runtime_model_6c, A_STOP<=39)
summary(trimet_runtime_model_6c$RUNTIME)

# Route 8
trimet_runtime_model_8 <- filter(trimet_runtime_model, ROUTE_NUMBER==8)
sd(trimet_runtime_model_8$A_STOP)*3 #16
summary(trimet_runtime_model_8$A_STOP)

trimet_runtime_model_8c <- filter(trimet_runtime_model_8, A_STOP>=1)
trimet_runtime_model_8c <- filter(trimet_runtime_model_8c, A_STOP<=33)
summary(trimet_runtime_model_8c$RUNTIME)

# Route 20
trimet_runtime_model_20 <- filter(trimet_runtime_model, ROUTE_NUMBER==20)
sd(trimet_runtime_model_20$A_STOP)*3 #30.43
summary(trimet_runtime_model_20$A_STOP)

trimet_runtime_model_20c <- filter(trimet_runtime_model_20, A_STOP>=12)
trimet_runtime_model_20c <- filter(trimet_runtime_model_20c, A_STOP<=73)
summary(trimet_runtime_model_20c$RUNTIME)

# Route 62
trimet_runtime_model_62 <- filter(trimet_runtime_model, ROUTE_NUMBER==62)
sd(trimet_runtime_model_62$A_STOP)*3 #14.51
summary(trimet_runtime_model_62$A_STOP)

trimet_runtime_model_62c <- filter(trimet_runtime_model_62, A_STOP>=2)
trimet_runtime_model_62c <- filter(trimet_runtime_model_62c, A_STOP<=30)
summary(trimet_runtime_model_62c$RUNTIME)

trimet_runtime_model_c <- rbind(trimet_runtime_model_6c, trimet_runtime_model_8c, 
                                trimet_runtime_model_20c, trimet_runtime_model_62c)

#write.csv(trimet_runtime_model_c, 'Runtime_Parts/trimet_runtime_model_c.2801_3101.csv')
#write.csv(trimet_runtime_model_c, 'Runtime_Parts/trimet_runtime_model_c.3102_3250.csv')
#write.csv(trimet_runtime_model_c, 'Runtime_Parts/trimet_runtime_model_c.3251_3620.csv')
#write.csv(trimet_runtime_model_c, 'Runtime_Parts/trimet_runtime_model_c.3621_3940.csv')
write.csv(trimet_runtime_model_c, 'Runtime_Parts/trimet_runtime_model_c.3941_4305.csv')

rm(list=ls())
gc()

# Bring it all home
trimet_runtime_1 <- read.csv('Runtime_Parts/trimet_runtime_model_c.2801_3101.csv')
trimet_runtime_2 <- read.csv('Runtime_Parts/trimet_runtime_model_c.3102_3250.csv')
trimet_runtime_3 <- read.csv('Runtime_Parts/trimet_runtime_model_c.3251_3620.csv')
trimet_runtime_4 <- read.csv('Runtime_Parts/trimet_runtime_model_c.3621_3940.csv')
trimet_runtime_5 <- read.csv('Runtime_Parts/trimet_runtime_model_c.3941_4305.csv')

trimet_runtime <- rbind(trimet_runtime_1, trimet_runtime_2, trimet_runtime_3,
                         trimet_runtime_4, trimet_runtime_5)

rm(trimet_runtime_1, trimet_runtime_2, trimet_runtime_3, trimet_runtime_4, trimet_runtime_5)

trimet_runtime <- subset(trimet_runtime, select=-c(X))

library(stringr)
trimet_runtime[c('DATE', 'OTHER')] <- str_split_fixed(trimet_runtime$SD_VN_TN_D, '_', 2)
trimet_runtime$DATE <- as.Date(trimet_runtime$DATE, format = "%Y-%m-%d")

trimet_weather <- read.csv("trimet_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_weather$DATE <- as.Date(trimet_weather$DATE, format = "%d-%b-%y")

trimet_runtime <- merge(trimet_runtime, trimet_weather, by='DATE')
write.csv(trimet_runtime, 'trimet_runtime.csv')

### RUNTIME MODEL ==============================================================
library(car)
library(stringr)

trimet_runtime_model <- read.csv('trimet_runtime.csv', header = TRUE, na.strings = c("","NA"))

# Welch's T-test
runtime_diesel <- trimet_runtime_complete %>% filter(EBUS==0) %>% subset(select=c(RUNTIME)) 
runtime_diesel <- runtime_diesel[['RUNTIME']]

runtime_ebus <- trimet_runtime_complete %>% filter(EBUS==1) %>% subset(select=c(RUNTIME))
runtime_ebus <- runtime_ebus[['RUNTIME']]

runtime_hybrid <- trimet_runtime_complete %>% filter(HYBRID==1) %>% subset(select=c(RUNTIME))
runtime_hybrid <- runtime_hybrid[['RUNTIME']]

t.test(runtime_ebus, runtime_hybrid, var.equal = F)
t.test(runtime_diesel, runtime_ebus, var.equal = F)

trimet_runtime_complete <- na.omit(trimet_runtime_model) # Remove NAs

trimet_runtime_complete[c('DATED', 'VEHICLE_NUMBER', 'OTHER')] <- str_split_fixed(trimet_runtime_complete$SD_VN_TN_D, '_', 3)
trimet_runtime_complete <- merge(trimet_runtime_complete, trimet_year, by='VEHICLE_NUMBER')

hybrid_sample <- filter(trimet_runtime_complete, HYBRID==1)
mean(hybrid_sample$AVG_TEMP)
sd(hybrid_sample$AVG_TEMP)

model_run <- lm(RUNTIME ~ A_STOP + AVG_LOAD + I(AVG_LOAD^2) + PACKS + I(PACKS^2) + 
                  INBOUND + OPERATOR_EXP + I(OPERATOR_EXP^2) + LIFT +
                  SCH_WEEK + factor(TIME_LEVELS) + factor(ROUTE) + 
                  PRECIP + AVG_TEMP*EBUS + PRECIP,
                  data = trimet_sample_run) #trimet_runtime_complete)
summary(model_run)
confint(model_run, level = 0.99)

vif(model2)

library(pastecs)
stat.desc(trimet_runtime_complete)

#correlation_check <- subset(trimet_runtime_model, select=-c(SD_VN_TN_D))
#cor(correlation_check)

### OLD MULTIPLE VEHICLES ======================================================

trimet$SD_VN_TN_D <- paste(trimet$SERVICE_DATE, trimet$VEHICLE_NUMBER, trimet$TRIP_NUMBER, trimet$DIRECTION, sep = '_')

# Remove duplicate stops
trimet_single <- distinct(trimet, SD_VN_TN_D, LOCATION_ID, .keep_all= TRUE) #Remove duplicates for the same stop


### OLD SURVIVAL LOGIT =========================================================
break_sample <- filter(trimet_breaks, ROADCALL_TYPE=='MAJ') # Only major breaks

#break_sample$DATE <- break_sample$date_event
#break_sample$DATE <- gsub("[[:punct:]]", "", break_sample$DATE)

names(break_sample)[names(break_sample)=='EQUIPMENT_COMPONENT_NUMBER'] <- 'VEHICLE_NUMBER'

break_sample <- unique(break_sample[c("VEHICLE_NUMBER", "date_event")]) # Remove when several breaks in one day

names(break_sample)[names(break_sample)=='date_event'] <- 'DATE'
break_sample$DATE_JOIN <- break_sample$DATE
#break_sample$DATE_JOIN <- as.character.Date(break_samplee$DATE)
break_sample[c('DAY', 'MONTH', 'YEAR')] <- str_split_fixed(break_sample$DATE_JOIN, '-', 3)
break_sample$DATE_MY <- paste(break_sample$MONTH, break_sample$YEAR, sep = '')
break_sample$VN_MY <- paste(break_sample$VEHICLE_NUMBER, break_sample$DATE_MY, sep = '_')
break_sample$DATE <- paste(break_sample$DAY, break_sample$MONTH, break_sample$YEAR, sep = '')
break_sample$BREAK <- c(1)

trimet_bus_type <- read.csv("Variables/trimet_bus_type.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_type <- subset(trimet_bus_type, select=-c(X))

trimet_bus_my <- read.csv("trimet_bus_my.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_my <- subset(trimet_bus_my, select=-c(X))
trimet_bus_my[c('VEHICLE_NUMBER', 'OTHER')] <- str_split_fixed(trimet_bus_my$VN_MY, '_', 2)

trimet_survival_ebus <- merge(trimet_bus_my, trimet_bus_type, by='VEHICLE_NUMBER')
trimet_survival_ebus_break <- merge(trimet_survival_ebus, break_sample, by='VN_MY', all.x=TRUE)

trimet_survival_ebus_break[is.na(trimet_survival_ebus_break)] <- 0
trimet_survival_ebus_break$VEHICLE_NUMBER <- trimet_survival_ebus_break$VEHICLE_NUMBER.x

trimet_survival_ebus_break <- subset(trimet_survival_ebus_break, select=c(VN_MY, VEHICLE_NUMBER, AVG_LOAD, DR_EXP, EBUS, ROUTE_NUMBER, BREAK))
trimet_survival_ebus_break$MONTH <- substring(trimet_survival_ebus_break$VN_MY, 6)


write.csv(trimet_survival_ebus_break, 'trimet_survival_ebus_break.csv')


trimet_weather <- read.csv("Variables/trimet_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_weather_t <- aggregate(AVG_TEMP ~ MONTH, data = trimet_weather, mean) 
trimet_weather_p <- aggregate(PRECIP ~ MONTH, data = trimet_weather, mean) 
trimet_weather_tp <- merge(trimet_weather_t, trimet_weather_p, by='MONTH')

trimet_survival_ebus_break_w <- merge(trimet_survival_ebus_break,trimet_weather_tp, by='MONTH')

write.csv(trimet_survival_ebus_break_w, 'trimet_survival_ebus_break_w.csv')

### OLD VARIABLES FOR SURVIVAL ANALYSIS ========================================
library(stringr)

# Create unique id based on vehicle number and date
trimet_single$BREAK <- trimet_single$SERVICE_DATE
trimet_single[c('DATE', 'OTHER')] <- str_split_fixed(trimet_single$BREAK, ':', 2)

trimet_single$DATE_1 <- substring(trimet_single$DATE, 3)
trimet_single$VN_SD <- paste(trimet_single$VEHICLE_NUMBER, trimet_single$DATE, sep = '_')

trimet_single$VN_MY <- paste(trimet_single$VEHICLE_NUMBER, trimet_single$DATE_1, sep = '_')


# FOR MONTH
# Route numbers
trimet_route <- subset(trimet_single,!duplicated(trimet_single$VN_MY))
trimet_route <- subset(trimet_route, select=c(VN_MY, ROUTE_NUMBER))

trimet_route$ROUTE_NUMBER <- as.factor(trimet_route$ROUTE_NUMBER)

levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='20'] <- '0'
levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='6'] <- '1'
levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='8'] <- '2'
levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='62'] <- '3'

# Electric or diesel bus
trimet_bus_type <- subset(trimet_single,!duplicated(trimet_single$VEHICLE_NUMBER))
trimet_bus_type$EBUS <- ifelse(trimet_bus_type$propulsion=='E', 1, 0)
trimet_bus_type <- subset(trimet_bus_type, select=c(VEHICLE_NUMBER, EBUS))
write.csv(trimet_bus_type, 'Variables/trimet_bus_type.csv')

# Driver experience
trimet_dr_exp <- na.omit(trimet_single)
trimet_dr_exp <- aggregate(operator_seniority ~ VN_MY, data = trimet_dr_exp, mean) # mean driver experience for the month
names(trimet_dr_exp)[names(trimet_dr_exp)=='operator_seniority'] <- 'DR_EXP'

# Average load
trimet_avg_load <- aggregate(ESTIMATED_LOAD ~ VN_MY, data = trimet_single, mean)
names(trimet_avg_load)[names(trimet_avg_load)=='ESTIMATED_LOAD'] <- 'AVG_LOAD'

# Merge
trimet_bus <- merge(trimet_route, trimet_avg_load, by='VN_MY')
trimet_bus <- merge(trimet_bus, trimet_dr_exp, by='VN_MY')

write.csv(trimet_bus, 'trimet_bus_my.csv')

# FOR DATE (No use)
# Route numbers
trimet_route <- subset(trimet,!duplicated(trimet$VN_SD))
trimet_route <- subset(trimet_route, select=c(VN_SD, ROUTE_NUMBER))

trimet_route$ROUTE_NUMBER <- as.factor(trimet_route$ROUTE_NUMBER)

levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='20'] <- '0'
levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='6'] <- '1'
levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='8'] <- '2'
levels(trimet_route$ROUTE_NUMBER)[levels(trimet_route$ROUTE_NUMBER)=='62'] <- '3'

# Electric or diesel bus
trimet_bus_type <- subset(trimet,!duplicated(trimet$VEHICLE_NUMBER))
trimet_bus_type$EBUS <- ifelse(trimet_bus_type$propulsion=='E', 1, 0)
trimet_bus_type <- subset(trimet_bus_type, select=c(VEHICLE_NUMBER, EBUS))
write.csv(trimet_bus_type, 'Variables/trimet_bus_type.csv')

# Driver experience
trimet_dr_exp <- subset(trimet,!duplicated(trimet$VN_SD))
trimet_dr_exp <- aggregate(operator_seniority ~ VN_SD, data = trimet_dr_exp, mean) # mean driver experience for the day
names(trimet_dr_exp)[names(trimet_dr_exp)=='operator_seniority'] <- 'DR_EXP'

# Average load
trimet_avg_load <- aggregate(ESTIMATED_LOAD ~ VN_SD, data = trimet, mean)
names(trimet_avg_load)[names(trimet_avg_load)=='ESTIMATED_LOAD'] <- 'AVG_LOAD'

# Merge
trimet_bus <- merge(trimet_dr_exp, trimet_avg_load, by='VN_SD')
trimet_bus[c('VEHICLE_NUMBER', 'DATE')] <- str_split_fixed(trimet_bus$VN_SD, '_', 2)
trimet_bus$VEHICLE_NUMBER <- as.integer(trimet_bus$VEHICLE_NUMBER)

trimet_bus <- merge(trimet_bus, trimet_bus_type, by='VEHICLE_NUMBER')
trimet_bus <- merge(trimet_bus, trimet_route, by='VN_SD')

# Add weather
trimet_weather <- read.csv("Variables/trimet_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_survival <- merge(trimet_bus, trimet_weather, by='DATE')

write.csv(trimet_survival, 'trimet_survival_bus_weather.csv')

### OLD BREAK DATA =============================================================
# Date values explained - https://www.statmethods.net/input/dates.html

break_sample <- filter(trimet_breaks, ROADCALL_TYPE=='MAJ') # Only major breaks

break_sample$DATE <- break_sample$date_event
break_sample$DATE <- gsub("[[:punct:]]", "", break_sample$DATE)

names(break_sample)[names(break_sample)=='EQUIPMENT_COMPONENT_NUMBER'] <- 'VEHICLE_NUMBER'

break_sample <- unique(break_sample[c("VEHICLE_NUMBER", "date_event")]) # Remove when several breaks in one day

break_sample$VN_SD <- paste(break_sample$VEHICLE_NUMBER, break_sample$DATE, sep = '_')
#break_sample <- subset(break_sample,!duplicated(break_sample$VN_SD)) # Remove when several breaks in one day
break_sample <- subset(break_sample, select=c(VEHICLE_NUMBER, date_event))

break_sample$date_event <- as.Date(break_sample$date_event, format = "%d-%B-%Y")

# Create a sample with clear time periods
break_sample_1b <- subset(break_sample,!duplicated(break_sample$VEHICLE_NUMBER)) #No duplicates

break_sample_2a <- subset(break_sample, duplicated(break_sample$VEHICLE_NUMBER))
break_sample_2b <- subset(break_sample_2a, !duplicated(break_sample_2a$VEHICLE_NUMBER)) #No duplicates

break_sample_3a <- subset(break_sample_2a, duplicated(break_sample_2a$VEHICLE_NUMBER))
break_sample_3b <- subset(break_sample_3a, !duplicated(break_sample_3a$VEHICLE_NUMBER)) #No duplicates

break_sample_4a <- subset(break_sample_3a, duplicated(break_sample_3a$VEHICLE_NUMBER))
break_sample_4b <- subset(break_sample_4a, !duplicated(break_sample_4a$VEHICLE_NUMBER)) #No duplicates

break_sample_5a <- subset(break_sample_4a, duplicated(break_sample_4a$VEHICLE_NUMBER))
break_sample_5b <- subset(break_sample_5a, !duplicated(break_sample_5a$VEHICLE_NUMBER)) #No duplicates

break_sample_6a <- subset(break_sample_5a, duplicated(break_sample_5a$VEHICLE_NUMBER))
break_sample_6b <- subset(break_sample_6a, !duplicated(break_sample_6a$VEHICLE_NUMBER)) #No duplicates

break_sample_7a <- subset(break_sample_6a, duplicated(break_sample_6a$VEHICLE_NUMBER))
break_sample_7b <- subset(break_sample_7a, !duplicated(break_sample_7a$VEHICLE_NUMBER)) #No duplicates

rm(break_sample_2a, break_sample_3a, break_sample_4a, break_sample_5a, break_sample_6a, break_sample_7a)

# Rename dates for unique headers
names(break_sample_1b)[names(break_sample_1b)=='date_event'] <- 'DATE_1'
names(break_sample_2b)[names(break_sample_2b)=='date_event'] <- 'DATE_2'
names(break_sample_3b)[names(break_sample_3b)=='date_event'] <- 'DATE_3'
names(break_sample_4b)[names(break_sample_4b)=='date_event'] <- 'DATE_4'
names(break_sample_5b)[names(break_sample_5b)=='date_event'] <- 'DATE_5'
names(break_sample_6b)[names(break_sample_6b)=='date_event'] <- 'DATE_6'
names(break_sample_7b)[names(break_sample_7b)=='date_event'] <- 'DATE_7'

# Create unique periods for sorting
break_sample_1b$PERIOD <- c(1)
break_sample_2b$PERIOD <- c(2)
break_sample_3b$PERIOD <- c(3)
break_sample_4b$PERIOD <- c(4)
break_sample_5b$PERIOD <- c(5)
break_sample_6b$PERIOD <- c(6)
break_sample_7b$PERIOD <- c(7)

# A) Period 1 time
break_sample_1b$DATE_0 <- c('05-SEP-2021') 
break_sample_1b$DATE_0 <- as.Date(break_sample_1b$DATE_0, format = "%d-%B-%Y")

break_sample_1b <- break_sample_1b %>% mutate(T.END = as.numeric(difftime(DATE_1, DATE_0,units = "days")))
break_sample_1b$EVENT <- c(1)
break_sample_1b$T.START <- c(0)

# B) Period 1 time (Adding censored records)
break_sample_1bt <- merge(break_sample_1b, break_sample_2b[ , c('VEHICLE_NUMBER', 'PERIOD')], 
                          by='VEHICLE_NUMBER', all.x = TRUE) # Merge to identify all that broke only during that period
break_sample_1bt <-  break_sample_1bt[is.na(break_sample_1bt$PERIOD.y),] # Leave only those that broke only during that period

break_sample_1bt$DATE_99 <- c('04-APR-2022')
break_sample_1bt$DATE_99 <- as.Date(break_sample_1bt$DATE_99, format = "%d-%B-%Y")
break_sample_1bt$T.START <- break_sample_1bt$T.END

break_sample_1bt <- break_sample_1bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_1bt$EVENT <- c(0)
break_sample_1bt$PERIOD <- c('2L')
break_sample_1bt$DATE_1 <- break_sample_1bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_1bt <- subset(break_sample_1bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_1c <- rbind(break_sample_1b, break_sample_1bt)

write.csv(break_sample_1c, 'Periods/break_sample_1c.csv')


# A) Period 2 time
break_sample_2b <- merge(break_sample_1b[ , c('VEHICLE_NUMBER', 'DATE_0', 'T.END')], break_sample_2b,                           
                         by='VEHICLE_NUMBER') # Merge to get the period start date
break_sample_2b$T.START <- break_sample_2b$T.END # Use the end of the previous period as a start for this
break_sample_2b <- break_sample_2b %>% mutate(T.END = as.numeric(difftime(DATE_2, DATE_0,units = "days")))
break_sample_2b$EVENT <- c(1)
names(break_sample_2b)[names(break_sample_2b)=='DATE_2'] <- 'DATE_1'

# B) Period 1 time (Adding censored records)
break_sample_2bt <- merge(break_sample_2b, break_sample_3b[ , c('VEHICLE_NUMBER', 'PERIOD')], 
                          by='VEHICLE_NUMBER', all.x = TRUE) # Merge to identify all that broke only during that period
break_sample_2bt <-  break_sample_2bt[is.na(break_sample_2bt$PERIOD.y),] # Leave only those that broke only during that period

break_sample_2bt$DATE_99 <- c('04-APR-2022')
break_sample_2bt$DATE_99 <- as.Date(break_sample_2bt$DATE_99, format = "%d-%B-%Y")
break_sample_2bt$T.START <- break_sample_2bt$T.END

break_sample_2bt <- break_sample_2bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_2bt$EVENT <- c(0)
break_sample_2bt$PERIOD <- c('3L')
break_sample_2bt$DATE_1 <- break_sample_2bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_2bt <- subset(break_sample_2bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_2c <- rbind(break_sample_2b, break_sample_2bt)

write.csv(break_sample_2c, 'Periods/break_sample_2c.csv')


# A) Period 3 time
break_sample_3b <- merge(break_sample_2b[ , c('VEHICLE_NUMBER', 'DATE_0', 'T.END')], break_sample_3b,                           
                         by='VEHICLE_NUMBER') # Merge to get the period start date
break_sample_3b$T.START <- break_sample_3b$T.END # Use the end of the previous period as a start for this
break_sample_3b <- break_sample_3b %>% mutate(T.END = as.numeric(difftime(DATE_3, DATE_0,units = "days")))
break_sample_3b$EVENT <- c(1)
names(break_sample_3b)[names(break_sample_3b)=='DATE_3'] <- 'DATE_1'

# B) Period 3 time (Adding censored records)
break_sample_3bt <- merge(break_sample_3b, break_sample_4b[ , c('VEHICLE_NUMBER', 'PERIOD')], 
                          by='VEHICLE_NUMBER', all.x = TRUE) # Merge to identify all that broke only during that period
break_sample_3bt <-  break_sample_3bt[is.na(break_sample_3bt$PERIOD.y),] # Leave only those that broke only during that period

break_sample_3bt$DATE_99 <- c('04-APR-2022')
break_sample_3bt$DATE_99 <- as.Date(break_sample_3bt$DATE_99, format = "%d-%B-%Y")
break_sample_3bt$T.START <- break_sample_3bt$T.END

break_sample_3bt <- break_sample_3bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_3bt$EVENT <- c(0)
break_sample_3bt$PERIOD <- c('4L')
break_sample_3bt$DATE_1 <- break_sample_3bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_3bt <- subset(break_sample_3bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_3c <- rbind(break_sample_3b, break_sample_3bt)

write.csv(break_sample_3c, 'Periods/break_sample_3c.csv')


# A) Period 4 time
break_sample_4b <- merge(break_sample_3b[ , c('VEHICLE_NUMBER', 'DATE_0', 'T.END')], break_sample_4b,                           
                         by='VEHICLE_NUMBER') # Merge to get the period start date
break_sample_4b$T.START <- break_sample_4b$T.END # Use the end of the previous period as a start for this
break_sample_4b <- break_sample_4b %>% mutate(T.END = as.numeric(difftime(DATE_4, DATE_0,units = "days")))
break_sample_4b$EVENT <- c(1)
names(break_sample_4b)[names(break_sample_4b)=='DATE_4'] <- 'DATE_1'

# B) Period 4 time (Adding censored records)
break_sample_4bt <- merge(break_sample_4b, break_sample_5b[ , c('VEHICLE_NUMBER', 'PERIOD')], 
                          by='VEHICLE_NUMBER', all.x = TRUE) # Merge to identify all that broke only during that period
break_sample_4bt <-  break_sample_4bt[is.na(break_sample_4bt$PERIOD.y),] # Leave only those that broke only during that period

break_sample_4bt$DATE_99 <- c('04-APR-2022')
break_sample_4bt$DATE_99 <- as.Date(break_sample_4bt$DATE_99, format = "%d-%B-%Y")
break_sample_4bt$T.START <- break_sample_4bt$T.END

break_sample_4bt <- break_sample_4bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_4bt$EVENT <- c(0)
break_sample_4bt$PERIOD <- c('5L')
break_sample_4bt$DATE_1 <- break_sample_4bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_4bt <- subset(break_sample_4bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_4c <- rbind(break_sample_4b, break_sample_4bt)

write.csv(break_sample_4c, 'Periods/break_sample_4c.csv')


# A) Period 5 time
break_sample_5b <- merge(break_sample_4b[ , c('VEHICLE_NUMBER', 'DATE_0', 'T.END')], break_sample_5b,                           
                         by='VEHICLE_NUMBER') # Merge to get the period start date
break_sample_5b$T.START <- break_sample_5b$T.END # Use the end of the previous period as a start for this
break_sample_5b <- break_sample_5b %>% mutate(T.END = as.numeric(difftime(DATE_5, DATE_0,units = "days")))
break_sample_5b$EVENT <- c(1)
names(break_sample_5b)[names(break_sample_5b)=='DATE_5'] <- 'DATE_1'

# B) Period 5 time (Adding censored records)
break_sample_5bt <- merge(break_sample_5b, break_sample_6b[ , c('VEHICLE_NUMBER', 'PERIOD')], 
                          by='VEHICLE_NUMBER', all.x = TRUE) # Merge to identify all that broke only during that period
break_sample_5bt <-  break_sample_5bt[is.na(break_sample_5bt$PERIOD.y),] # Leave only those that broke only during that period

break_sample_5bt$DATE_99 <- c('04-APR-2022')
break_sample_5bt$DATE_99 <- as.Date(break_sample_5bt$DATE_99, format = "%d-%B-%Y")
break_sample_5bt$T.START <- break_sample_5bt$T.END

break_sample_5bt <- break_sample_5bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_5bt$EVENT <- c(0)
break_sample_5bt$PERIOD <- c('6L')
break_sample_5bt$DATE_1 <- break_sample_5bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_5bt <- subset(break_sample_5bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_5c <- rbind(break_sample_5b, break_sample_5bt)

write.csv(break_sample_5c, 'Periods/break_sample_5c.csv')


# A) Period 6 time
break_sample_6b <- merge(break_sample_5b[ , c('VEHICLE_NUMBER', 'DATE_0', 'T.END')], break_sample_6b,                           
                         by='VEHICLE_NUMBER') # Merge to get the period start date
break_sample_6b$T.START <- break_sample_6b$T.END # Use the end of the previous period as a start for this
break_sample_6b <- break_sample_6b %>% mutate(T.END = as.numeric(difftime(DATE_6, DATE_0,units = "days")))
break_sample_6b$EVENT <- c(1)
names(break_sample_6b)[names(break_sample_6b)=='DATE_6'] <- 'DATE_1'

# B) Period 6 time (Adding censored records)
break_sample_6bt <- merge(break_sample_6b, break_sample_7b[ , c('VEHICLE_NUMBER', 'PERIOD')], 
                          by='VEHICLE_NUMBER', all.x = TRUE) # Merge to identify all that broke only during that period
break_sample_6bt <-  break_sample_6bt[is.na(break_sample_6bt$PERIOD.y),] # Leave only those that broke only during that period

break_sample_6bt$DATE_99 <- c('04-APR-2022')
break_sample_6bt$DATE_99 <- as.Date(break_sample_6bt$DATE_99, format = "%d-%B-%Y")
break_sample_6bt$T.START <- break_sample_6bt$T.END

break_sample_6bt <- break_sample_6bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_6bt$EVENT <- c(0)
break_sample_6bt$PERIOD <- c('7L')
break_sample_6bt$DATE_1 <- break_sample_6bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_6bt <- subset(break_sample_6bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_6c <- rbind(break_sample_6b, break_sample_6bt)

write.csv(break_sample_6c, 'Periods/break_sample_6c.csv')


# A) Period 7 time (Doesn't have censored records - last period)
break_sample_7b <- merge(break_sample_6b[ , c('VEHICLE_NUMBER', 'DATE_0', 'T.END')], break_sample_7b,                           
                         by='VEHICLE_NUMBER') # Merge to get the period start date
break_sample_7b$T.START <- break_sample_7b$T.END # Use the end of the previous period as a start for this
break_sample_7b <- break_sample_7b %>% mutate(T.END = as.numeric(difftime(DATE_7, DATE_0,units = "days")))
break_sample_7b$EVENT <- c(1)
names(break_sample_7b)[names(break_sample_7b)=='DATE_7'] <- 'DATE_1'

# B) Period 6 time (Adding censored records)
break_sample_7bt <- break_sample_7b

break_sample_7bt$DATE_99 <- c('04-APR-2022')
break_sample_7bt$DATE_99 <- as.Date(break_sample_7bt$DATE_99, format = "%d-%B-%Y")
break_sample_7bt$T.START <- break_sample_7bt$T.END

break_sample_7bt <- break_sample_7bt %>% mutate(T.END = as.numeric(difftime(DATE_99, DATE_0,units = "days")))
break_sample_7bt$EVENT <- c(0)
break_sample_7bt$PERIOD <- c('8L')
break_sample_7bt$DATE_1 <- break_sample_7bt$DATE_99 #Date for the censored event is the last date in the study

break_sample_7bt <- subset(break_sample_7bt, select=c(VEHICLE_NUMBER, DATE_1, PERIOD, DATE_0, T.END, EVENT, T.START))

break_sample_7c <- rbind(break_sample_7b, break_sample_7bt)

write.csv(break_sample_7c, 'Periods/break_sample_7c.csv')


# Bind all periods
break_sample_survive <- rbind(break_sample_1c, break_sample_2c, break_sample_3c, 
                              break_sample_4c, break_sample_5c, break_sample_6c, break_sample_7c)

break_sample_survive <- subset(break_sample_survive, select=-c(DATE_0))
names(break_sample_survive)[names(break_sample_survive)=='DATE_1'] <-'DATE'

break_sample_survive <- break_sample_survive[, c(1, 2, 3, 6, 4, 5)]
break_sample_survive$TIME <- break_sample_survive$T.END - break_sample_survive$T.START

# Prepare a date preceding the break (For operational data)
library(stringr)

# For reoccuring analysis
break_sample_survive$DATE_JOIN <- break_sample_survive$DATE
break_sample_survive$DATE_JOIN <- as.character.Date(break_sample_survive$DATE_JOIN)
break_sample_survive[c('YEAR', 'MONTH_1', 'DATE_1')] <- str_split_fixed(break_sample_survive$DATE_JOIN, '-', 3)

break_sample_survive$DATE_0 <- as.integer(break_sample_survive$DATE_1)
break_sample_survive$DATE_0 <- break_sample_survive$DATE_0 - 1 

# Change year and month for the day preceding
break_sample_survive$MONTH_0 <- as.integer(break_sample_survive$MONTH_1)
break_sample_survive$YEAR_0 <- as.integer(break_sample_survive$YEAR)
break_sample_survive$YEAR_0 <- ifelse(break_sample_survive$DATE_0==0 & break_sample_survive$MONTH_0==1, 
                                       break_sample_survive$YEAR_0-1, break_sample_survive$YEAR_0)

break_sample_survive$MONTH_0 <- ifelse(break_sample_survive$DATE_0==0, break_sample_survive$MONTH_0-1, 
                                       break_sample_survive$MONTH_0)
break_sample_survive$MONTH_0 <- ifelse(break_sample_survive$MONTH_0==0, 12, 
                                       break_sample_survive$MONTH_0)

break_sample_survive$DATE_0 <- ifelse(break_sample_survive$DATE_0==0, 31, 
                                       break_sample_survive$DATE_0)
break_sample_survive$DATE_0 <- ifelse(break_sample_survive$DATE_0==31 & break_sample_survive$MONTH_0==2, 28, 
                                      break_sample_survive$DATE_0)
break_sample_survive$DATE_0 <- ifelse(break_sample_survive$DATE_0==31 & break_sample_survive$MONTH_0==8, 30, 
                                      break_sample_survive$DATE_0)
break_sample_survive$DATE_0 <- ifelse(break_sample_survive$DATE_0==31 & break_sample_survive$MONTH_0==10, 30, 
                                      break_sample_survive$DATE_0)

break_sample_survive$DATE_0 <- as.character(break_sample_survive$DATE_0)
break_sample_survive$DATE_0 <- ifelse(nchar(break_sample_survive$DATE_0)==1, 
                                     paste0('0',break_sample_survive$DATE_0), break_sample_survive$DATE_0)

break_sample_survive$MONTH_0 <- as.character(break_sample_survive$MONTH_0)
break_sample_survive$MONTH_0 <- ifelse(nchar(break_sample_survive$MONTH_0)==1, 
                                      paste0('0',break_sample_survive$MONTH_0), break_sample_survive$MONTH_0)

break_sample_survive$MONTH_1 <- as.factor(break_sample_survive$MONTH_1)
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='01'] <- 'JAN' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='02'] <- 'FEB' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='03'] <- 'MAR' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='04'] <- 'APR' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='09'] <- 'SEP' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='10'] <- 'OCT' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='11'] <- 'NOV' 
levels(break_sample_survive$MONTH_1)[levels(break_sample_survive$MONTH_1)=='12'] <- 'DEC' 

break_sample_survive$MONTH_0 <- as.factor(break_sample_survive$MONTH_0)
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='01'] <- 'JAN' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='02'] <- 'FEB' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='03'] <- 'MAR' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='04'] <- 'APR' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='09'] <- 'SEP' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='10'] <- 'OCT' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='11'] <- 'NOV' 
levels(break_sample_survive$MONTH_0)[levels(break_sample_survive$MONTH_0)=='12'] <- 'DEC' 


break_sample_survive$DATE_M1 <- paste(break_sample_survive$DATE_0, break_sample_survive$MONTH_0, 
                                      break_sample_survive$YEAR_0, sep = '')
break_sample_survive$DATE_J1 <- paste(break_sample_survive$DATE_1, break_sample_survive$MONTH_1, 
                                      break_sample_survive$YEAR, sep = '')

break_sample_survive$DATE_MY <- paste(break_sample_survive$MONTH_1, 
                                      break_sample_survive$YEAR, sep = '')

break_sample_survive <- subset(break_sample_survive, select=-c(DATE_JOIN, YEAR, YEAR_0, MONTH_1, MONTH_0, DATE_1, DATE_0))

write.csv(break_sample_survive, 'Periods/break_sample_survive_breaks_only.csv')

# Merge breaks and other variables
break_sample_survive_breaks_only <- read.csv("Periods/break_sample_survive_breaks_only.csv", 
                                             header = TRUE, na.strings = c("","NA"))
break_sample_survive_breaks_only <- subset(break_sample_survive_breaks_only, select=-c(X))

trimet_bus_type <- read.csv("Variables/trimet_bus_type.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_type <- subset(trimet_bus_type, select=-c(X))

trimet_survival_ebus <- merge(break_sample_survive_breaks_only, trimet_bus_type, by='VEHICLE_NUMBER')

trimet_weather <- read.csv("Variables/trimet_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_survival_ebus_weather <- merge(trimet_survival_ebus, trimet_weather, by.x='DATE_J1', by.y='DATE')

write.csv(trimet_survival_ebus_weather, 'trimet_survival_ebus_weather.csv')



# Merge by date (No use)
break_sample_survive_breaks_only$VN_SD_J1 <- paste(break_sample_survive_breaks_only$VEHICLE_NUMBER, 
                                                   break_sample_survive_breaks_only$DATE_J1, sep = '_')
break_sample_survive_breaks_only$VN_SD_M1 <- paste(break_sample_survive_breaks_only$VEHICLE_NUMBER, 
                                                   break_sample_survive_breaks_only$DATE_M1, sep = '_')
trimet_bus_weather_exp <- read.csv("trimet_bus_weather_exp.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_weather_exp <- subset(trimet_bus_weather_exp, select=-c(X))
trimet_survival <- merge(break_sample_survive_breaks_only, trimet_bus_weather_exp, by.x='VN_SD_J1', by.y='VN_SD')

# For negative binomial
break_sample$BREAK <- c(1)
trimet_survival_nb <- aggregate(BREAK ~ VEHICLE_NUMBER, data = break_sample, sum)

trimet_survival_nb <- merge(trimet_survival_nb, trimet_bus_type, by='VEHICLE_NUMBER')

trimet_ebus <- merge(break_sample, trimet_bus_type, by='VEHICLE_NUMBER')

### OLD SURVIVAL ANALYSIS ======================================================

# Negative binomial regression
library(MASS)
model <- glm.nb(BREAK ~ MAINT_MILES + MILES*EBUS + A_STOP + AVG_LOAD + 
                       LIFT_DWELL, data = trimet_days_dep_C)
summary(model)

# Logit
trimet_survival_ebus_break_w <- read.csv("trimet_survival_ebus_break_w.csv", header = TRUE, na.strings = c("","NA"))
trimet_survival_ebus_break_w <- subset(trimet_survival_ebus_break_w, select=-c(X))

#trimet_survival_ebus_break_w$MONTH <- as.factor(trimet_survival_ebus_break_w$MONTH)
#trimet_survival_ebus_break_w <- within(trimet_survival_ebus_break_w, MONTH <- relevel(MONTH, ref = 'DEC2021')) #Reorder fators

model <- glm(BREAK ~ EBUS*DR_EXP + AVG_LOAD + factor(ROUTE_NUMBER) + factor(MONTH), family=binomial(link='logit'),data=trimet_survival_ebus_break_w)
summary(model)

model0<-update(model, formula= BREAK ~ 1)
McFadden<- 1-as.vector(logLik(model)/logLik(model0))
McFadden

# Reocurring Survival 
library(reReg)

trimet_survival_ebus_weather <- read.csv("trimet_survival_ebus_weather.csv", header = TRUE, na.strings = c("","NA"))
trimet_survival_ebus_weather <- subset(trimet_survival_ebus_weather, select=-c(X))

trimet_survival_ebus_weather$VN_MY <- paste(trimet_survival_ebus_weather$VEHICLE_NUMBER, 
                                            trimet_survival_ebus_weather$DATE_MY, sep = '_')

trimet_bus_my <- read.csv("trimet_bus_my.csv", header = TRUE, na.strings = c("","NA"))
trimet_bus_my <- subset(trimet_bus_my, select=-c(X))

trimet_survival_ebus_weather_bus <- merge(trimet_survival_ebus_weather, trimet_bus_my, by='VN_MY')

t<- filter(trimet_survival_ebus_weather_nd, T.START!=T.END)



plot(reReg(Recur(t.start %to% t.stop, id, event, status) ~ 1, data = simDat, B = 50))
fm <- Recur(t.start %to% t.stop, id, event) ~ 1
summary(reReg(fm, data = simDat, model = "cox", B = 50))


reObj <- with(trimet_survival_ebus_weather, Recur(T.START %to% T.END, id=VEHICLEanalysis_NUMBER, event=EVENT)) # t.start=T.START, t.stop
plot(reObj)

fit <- reReg(Recur(T.START %to% T.END, id=VEHICLE_NUMBER, event=EVENT) ~ 1, 
             data = trimet_survival_ebus_weather) #, method = "cox.LWYY")


# Survival analysis
library(survival)
library(ggfortify)

#trimet_survival_ebus_weather_nd <- subset(trimet_survival_ebus_weather_bus,!duplicated(trimet_survival_ebus_weather$VEHICLE_NUMBER))
trimet_survival_ebus_weather_nd <- trimet_survival_ebus_weather_bus %>% group_by(VEHICLE_NUMBER) %>% filter(DATE==min(DATE))

km_fit <- survfit(Surv(TIME, EVENT) ~ EBUS, data=trimet_survival_ebus_weather_nd)
summary(km_fit)
autoplot(km_fit)
ggsave("Trimet_Kaplan-Meier.jpg", width = 8, height = 5.5)

cox <- coxph(Surv(T.START, T.END, EVENT) ~ EBUS, data=t) # + AVG_LOAD:T.END + AVG_TEMP:T.END + PRECIP:T.END, 
summary(cox)

sandbox_data <- survSplit(Surv(T.START, T.END, EVENT) ~ EBUS,
                          data = t, cut = c(70,150), episode = "tgroup", id = "VEHICLE_NUMBER")

cox2 <- coxph(Surv(T.START, T.END, EVENT) ~ EBUS:strata(tgroup), data=sandbox_data) # + AVG_LOAD:T.END + AVG_TEMP:T.END + PRECIP:T.END, 
summary(cox2)


zph <- cox.zph(cox2)
zph

plot(zph)


### OLD VARIABLES FOR RUNTIME MODEL ============================================

# Runtime
#Routes <- c(1114, 1026, 868, 9573, 1273, 1293, 5028, 13660, 13266, 12008, 8199,  9654, 10857, 9625, 9651)

trimet_start <- filter(trimet_single, SCHEDULE_STATUS==5) #First Stop
#n_occur <- data.frame(table(trimet_finish$SD_VN_TN_D)) #Search for duplicates
#n_occur[n_occur$Freq > 1,]

#trimet_start <- filter(trimet_start, LOCATION_ID %in% Routes)
trimet_start <- subset(trimet_start, select=c(SD_VN_TN_D, LEAVE_TIME, ROUTE_NUMBER))

trimet_finish <- filter(trimet_single, SCHEDULE_STATUS==6) #Last Stop
#n_occur <- data.frame(table(trimet_finish$SD_VN_TN_D)) #Search for duplicates
#n_occur[n_occur$Freq > 1,]

#trimet_finish <- filter(trimet_finish, LOCATION_ID %in% Routes)
trimet_finish <- subset(trimet_finish, select=c(SD_VN_TN_D, ARRIVE_TIME))

trimet_runtime <-merge(trimet_start, trimet_finish, by='SD_VN_TN_D')
trimet_runtime <- mutate(trimet_runtime, RUNTIME=ARRIVE_TIME-LEAVE_TIME)
#trimet_runtime$RUNTIME_M <- trimet_runtime$RUNTIME/60
write.csv(trimet_runtime, 'Variables/trimet_runtime.csv')
rm(trimet_start, trimet_finish)

# Number of stops
#trimet_total_stops <- trimet_single %>% filter(SCHEDULE_STATUS <=2 | SCHEDULE_STATUS >=5) %>%
#                      group_by(SD_VN_TN_D) %>% summarise(count = n_distinct(LOCATION_ID))
#names(trimet_total_stops)[names(trimet_total_stops)=='count'] <- 'ACTUAL_STOPS'

trimet_single$A_STOP <- ifelse(trimet_single$DWELL>=1, 1, 0) #If DWELL was more than 0 (Door opened)
trimet_actual_stops <- aggregate(A_STOP ~ SD_VN_TN_D, data = trimet_single, sum)

write.csv(trimet_actual_stops, 'Variables/trimet_actual_stops.csv')

# Total Ons and Offs

trimet_ons <- aggregate(ONS ~ SD_VN_TN_D, data = trimet_single_nfl, sum)
trimet_offs <- aggregate(OFFS ~ SD_VN_TN_D, data = trimet_single_nfl, sum)

trimet_ons_offs <- merge(trimet_ons, trimet_offs, by='SD_VN_TN_D') #Note: 0.97 correlation
trimet_ons_offs$PAX <- trimet_ons_offs$ONS + trimet_ons_offs$OFFS
trimet_ons_offs$PAX2 <- trimet_ons_offs$PAX^2
write.csv(trimet_ons_offs, 'Variables/trimet_ons_offs.csv')

# Average and max load
trimet_avg_load <- aggregate(ESTIMATED_LOAD ~ SD_VN_TN_D, data = trimet_single_nfl, mean)
names(trimet_avg_load)[names(trimet_avg_load)=='ESTIMATED_LOAD'] <- 'AVG_LOAD'

trimet_max_load <- aggregate(ESTIMATED_LOAD ~ SD_VN_TN_D, data = trimet_single_nfl, max)
names(trimet_max_load)[names(trimet_max_load)=='ESTIMATED_LOAD'] <- 'MAX_LOAD'

trimet_loads <- merge(trimet_max_load, trimet_avg_load, by='SD_VN_TN_D')
write.csv(trimet_loads, 'Variables/trimet_loads.csv')

# Time of day
trimet_time <- filter(trimet_single, SCHEDULE_STATUS==5) #First Stop
trimet_time <- subset(trimet_time, select=c(SD_VN_TN_D, LEAVE_TIME))
trimet_time$START_H <- trimet_time$LEAVE_TIME/3600

trimet_time$TIME_LEVELS <-ifelse(trimet_time$START_H<=6.50, 1, 
                                 ifelse(trimet_time$START_H>6.50 & trimet_time$START_H<=9.50 , 2, 
                                        ifelse(trimet_time$START_H>9.50 & trimet_time$START_H<=15.50 , 3, 
                                               ifelse(trimet_time$START_H>15.50 & trimet_time$START_H<=18.50 , 4, 
                                                      0))))


#trimet_time$TIME_EAM <-ifelse(trimet_time$START_H<=6.50, 1, 0) #3-6:30
#trimet_time$TIME_PAM <-ifelse(trimet_time$START_H>6.50 & trimet_time$START_H<=9.50 , 1, 0) #6:30-9:30
#trimet_time$TIME_MID <-ifelse(trimet_time$START_H>9.50 & trimet_time$START_H<=15.50 , 1, 0) #9:30-3:30
#trimet_time$TIME_PPM <-ifelse(trimet_time$START_H>15.50 & trimet_time$START_H<=18.50 , 1, 0) #3:30-6:30
#trimet_time$TIME_EVEN <-ifelse(trimet_time$START_H>18.50, 1, 0) #6:30-3

trimet_time <- subset(trimet_time, select=-c(START_H,LEAVE_TIME))

write.csv(trimet_time, 'Variables/trimet_time_of_day.csv')

# Route, Direction, Service, Experience
trimet_rdle <- filter(trimet_single, SCHEDULE_STATUS==5) #First Stop
trimet_rdle <- subset(trimet_rdle, select=c(SD_VN_TN_D, ROUTE_NUMBER, DIRECTION, SERVICE_KEY, operator_seniority, propulsion))

trimet_rdle$ROUTE_NUMBER <- as.factor(trimet_rdle$ROUTE_NUMBER)

levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='20'] <- '0'
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='6'] <- '1'
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='8'] <- '2'
levels(trimet_rdle$ROUTE_NUMBER)[levels(trimet_rdle$ROUTE_NUMBER)=='62'] <- '3'

names(trimet_rdle)[names(trimet_rdle)=='ROUTE_NUMBER'] <- 'ROUTE'

#trimet_rdle$ROUTE_6 <- ifelse(trimet_rdle$ROUTE_NUMBER==6, 1, 0)
#trimet_rdle$ROUTE_8 <- ifelse(trimet_rdle$ROUTE_NUMBER==8, 1, 0)
#trimet_rdle$ROUTE_20 <- ifelse(trimet_rdle$ROUTE_NUMBER==20, 1, 0)
#trimet_rdle$ROUTE_62 <- ifelse(trimet_rdle$ROUTE_NUMBER==62, 1, 0)

trimet_rdle$SCH_WEEK <- ifelse(trimet_rdle$SERVICE_KEY=='W', 1, 0)
trimet_rdle$SCH_SAT <- ifelse(trimet_rdle$SERVICE_KEY=='S', 1, 0)
trimet_rdle$SCH_SUN <- ifelse(trimet_rdle$SERVICE_KEY=='U', 1, 0)
trimet_rdle$SCH_HOL <- ifelse(trimet_rdle$SERVICE_KEY=='X', 1, 0)

names(trimet_rdle)[names(trimet_rdle)=='DIRECTION'] <- 'INBOUND'
names(trimet_rdle)[names(trimet_rdle)=='operator_seniority'] <- 'OPERATOR_EXP'
trimet_rdle$OPERATOR_EXP2 <- trimet_rdle$OPERATOR_EXP^2
trimet_rdle$EBUS <- ifelse(trimet_rdle$propulsion=='E', 1, 0)

trimet_rdle <- subset(trimet_rdle, select=-c(SERVICE_KEY, propulsion))

write.csv(trimet_rdle, 'Variables/trimet_rdle.csv')

# Lift
trimet_lift <- aggregate(LIFT ~ SD_VN_TN_D, data = trimet_single, sum)
write.csv(trimet_lift, 'Variables/trimet_lift.csv')

# Weather

#Prep
library(stringr)

#trimet_runtime_model$BREAK <- trimet_runtime_model$SD_VN_TN_D
#trimet_runtime_model[c('DATE', 'OTHER')] <- str_split_fixed(trimet_runtime_model$BREAK, ':', 2)
#trimet_date <- table(trimet_runtime$DATE)

#write.csv(trimet_date, 'Variables/trimet_date.csv')

#Merge with weather from https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00024229/detail

trimet_runtime$BREAK <- trimet_runtime$SD_VN_TN_D
trimet_runtime[c('DATE', 'OTHER')] <- str_split_fixed(trimet_runtime$BREAK, ':', 2)
trimet_runtime$DATE <- as.Date(trimet_runtime$DATE)

trimet_weather <- read.csv("Variables/trimet_weather.csv", header = TRUE, na.strings = c("","NA"))

trimet_runtime <- merge(trimet_runtime, trimet_weather, by='DATE')
trimet_runtime <- subset(trimet_runtime, select=-c(BREAK, OTHER, DAY, MONTH))

write.csv(trimet_runtime, 'Variables/trimet_runtime_weather.csv')

# Merge all
trimet_runtime_model <- merge(trimet_runtime, trimet_loads, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_lift, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_ons_offs, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_rdle, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_time, by='SD_VN_TN_D')
trimet_runtime_model <- merge(trimet_runtime_model, trimet_actual_stops, by='SD_VN_TN_D')

# Route 6
trimet_runtime_model_6 <- filter(trimet_runtime_model, ROUTE_NUMBER==6)
table(trimet_runtime_model_6$A_STOP)
quantile(trimet_runtime_model_6$A_STOP, c(0.1, 0.9))

sd(trimet_runtime_model_6$A_STOP)*3 #17.32
summary(trimet_runtime_model_6$A_STOP)

trimet_runtime_model_6c <- filter(trimet_runtime_model_6, A_STOP>=4)
trimet_runtime_model_6c <- filter(trimet_runtime_model_6c, A_STOP<=39)
summary(trimet_runtime_model_6c$RUNTIME)

# Route 8
trimet_runtime_model_8 <- filter(trimet_runtime_model, ROUTE_NUMBER==8)
summary(trimet_runtime_model_8$RUNTIME)
table(trimet_runtime_model_8$A_STOP)

sd(trimet_runtime_model_8$A_STOP)*3 #16.04
summary(trimet_runtime_model_8$A_STOP)

trimet_runtime_model_8c <- filter(trimet_runtime_model_8, A_STOP>=1)
trimet_runtime_model_8c <- filter(trimet_runtime_model_8c, A_STOP<=33)
summary(trimet_runtime_model_8c$RUNTIME)

# Route 20
trimet_runtime_model_20 <- filter(trimet_runtime_model, ROUTE_NUMBER==20)
summary(trimet_runtime_model_20$RUNTIME)
table(trimet_runtime_model_20$A_STOP)

sd(trimet_runtime_model_20$A_STOP)*3 #28.96
summary(trimet_runtime_model_20$A_STOP)

trimet_runtime_model_20c <- filter(trimet_runtime_model_20, A_STOP>=13)
trimet_runtime_model_20c <- filter(trimet_runtime_model_20c, A_STOP<=71)
summary(trimet_runtime_model_20c$RUNTIME)

# Route 62

trimet_runtime_model_62 <- filter(trimet_runtime_model, ROUTE_NUMBER==62)
summary(trimet_runtime_model_62$RUNTIME)
table(trimet_runtime_model_62$A_STOP)

sd(trimet_runtime_model_62$A_STOP)*3 #14.51
summary(trimet_runtime_model_62$A_STOP)

trimet_runtime_model_62c <- filter(trimet_runtime_model_62, A_STOP>=1)
trimet_runtime_model_62c <- filter(trimet_runtime_model_62c, A_STOP<=30)
summary(trimet_runtime_model_62c$RUNTIME)

trimet_runtime_model_c <- rbind(trimet_runtime_model_6c, trimet_runtime_model_8c, 
                                trimet_runtime_model_20c, trimet_runtime_model_62c)


# Save

trimet_runtime_model <- subset(trimet_runtime_model, select=-c(LEAVE_TIME, ARRIVE_TIME))

write.csv(trimet_runtime_model, 'trimet_runtime_model.csv')

write.csv(trimet_runtime_model_c, 'trimet_runtime_model_c.csv')

#correlation_check <- subset(trimet_runtime_model, select=-c(SD_VN_TN_D))
#cor(correlation_check)

### TESTING ====================================================================


# Box-Cox Transformation
library(MASS)

b <- boxcox(lm(BREAK_PERC ~ 1, data = trimet_days_dep_nz))
lambda <- b$x[which.max(b$y)] # 0.0606 # Exact lambda

trimet_days_dep_nz$BREAK_PERC_BC <- (trimet_days_dep_nz$BREAK_PERC ^ lambda - 1) / lambda

bc_break <- BoxCoxTrans(trimet_days_dep$BREAK_PERC, )
trimet_days_dep <- cbind(trimet_days_dep, BREAK_PERC_BC=predict(bc_break, trimet_days_dep$BREAK_PERC)) # append the transformed variable
head(trimet_days_dep) # view the top 6 rows




# Break data

trimet_breaks <- read.csv("sep 2021 vehicle failures.csv", header = TRUE, na.strings = c("","NA"))

#n_occur <- data.frame(table(trimet_breaks_major$EQUIPMENT_COMPONENT_NUMBER)) #Search for duplicates
#count <- n_occur[n_occur$Freq > 0,]

break_sample <- filter(trimet_breaks, ROADCALL_TYPE=='MAJ') # Only major breaks

break_sample$DATE <- break_sample$date_event
break_sample$DATE <- gsub("[[:punct:]]", "", break_sample$DATE)

names(break_sample)[names(break_sample)=='EQUIPMENT_COMPONENT_NUMBER'] <- 'VEHICLE_NUMBER'

break_sample$date_start <- c('05-SEP-2021')

# Date values explained - https://www.statmethods.net/input/dates.html

break_sample$date_start <- as.Date(break_sample$date_start, format = "%d-%B-%Y")
break_sample$date_event <- as.Date(break_sample$date_event, format = "%d-%B-%Y")
break_sample <- break_sample %>% mutate(TIME = as.numeric(difftime(date_event, date_start,units = "days")))

break_sample_nodp <- break_sample %>% group_by(VEHICLE_NUMBER) %>% 
                                      filter(date_event == max(date_event)) %>% distinct #A sample with a first break only
break_sample_nodp <- subset(break_sample_nodp,!duplicated(break_sample_nodp$VEHICLE_NUMBER))

trimet_survival_cox <- merge(break_sample_nodp, trimet_weather, by='DATE')
trimet_survival_cox$STATUS <- c(1)
trimet_survival_cox$STATUS_100 <- ifelse(trimet_survival_cox$TIME<=100, 1, 0)
trimet_survival_cox <- merge(trimet_survival_cox, trimet_bus_type, by='VEHICLE_NUMBER')

# For logit model (no use)
break_sample <- subset(break_sample, select=c(VN_SD))
break_sample$BREAK <- c(1)

trimet_survival_logit <- merge(trimet_survival, break_sample, by='VN_SD', all.x = TRUE)
trimet_survival_logit[is.na(trimet_survival_logit)] <- 0

table(trimet_breaks$DESCRIPTION)

# Test trips/Routes
test_10 <- filter(trimet, SD_TN_D=='05SEP2021:00:00:00_1090_0')
test_11 <- filter(trimet, SD_TN_D=='05SEP2021:00:00:00_1090_1')
write.csv(test_10, 'trimet_62_O.csv')
write.csv(test_11, 'trimet_62_I.csv')

test_20 <- filter(trimet, SD_TN_D=='05SEP2021:00:00:00_1320_0')
test_21 <- filter(trimet, SD_TN_D=='05SEP2021:00:00:00_1320_1')
write.csv(test_20, 'trimet_6_O.csv')
write.csv(test_21, 'trimet_6_I.csv')

test_duplicates <- filter(trimet, SD_TN_D=='01FEB2022:00:00:00_1605_0')
test_duplicates2 <- filter(trimet_single, SD_VN_TN_D=='01APR2022:00:00:00_2939_1110_0')

test_duplicates_r <- distinct(test_duplicates, LOCATION_ID, .keep_all= TRUE) #Remove duplicates
trimet_start <- distinct(trimet_start, SD_TN_D, .keep_all= TRUE) #Remove duplicates

n_occur <- data.frame(table(trimet_start$SD_TN_D)) #Search for duplicates
n_occur[n_occur$Freq > 1,]

trimet_breaks_type <- merge(trimet_breaks,trimet_bus_type, by.x='EQUIPMENT_COMPONENT_NUMBER', by.y='VEHICLE_NUMBER')

# Service days summary stats
trimet_days <- table(trimet$VEHICLE_NUMBER,trimet$SERVICE_DATE)
trimet_days <- as.data.frame(trimet_days)
trimet_days <- filter(trimet_days, Freq!=0)

trimet_days$Var1 <- as.character(trimet_days$Var1)

table_days <- as.data.frame(table(trimet_days$Var1))
summary(table_days$Freq)

# Dates in sesrvice, dates in maintenance
trimet_days <- distinct(trimet_single, VEHICLE_NUMBER, DATE) #Only unique days sample
n_distinct(trimet_days$VEHICLE_NUMBER)

# Count of gaps in service (more than a day difference)
trimet_gaps <- trimet_days %>%
  group_by(VEHICLE_NUMBER) %>%
  arrange(DATE, .by_group = TRUE) %>%
  mutate(GAP_DAYS = 
           as.numeric(difftime(DATE, lag(DATE, default = first(DATE)), units = "days"))) # Count number of continuous service days

trimet_gaps$MAINT_N <- ifelse(trimet_gaps$GAP_DAYS>=2, 1, 0) # Count each gap longer than a day
trimet_gaps <- filter(trimet_gaps, MAINT_N!=0) # Keep only maintenance days
trimet_gaps <- data.table(trimet_gaps, key = c("VEHICLE_NUMBER", "DATE")) # Sort by vehicle number and date of maintenance
trimet_gaps_all <- trimet_gaps # A sample with all maintenance occurrences

trimet_gaps <- distinct(trimet_gaps, VEHICLE_NUMBER, .keep_all= TRUE)

trimet_gaps <- merge(trimet_gaps, break_first, by='VEHICLE_NUMBER') # Join with break data
trimet_gaps <- filter(trimet_gaps, DATE<=DATE_BREAK) # Filter out breaks before the first maintenance
trimet_gaps <- data.table(trimet_gaps, key = c("VEHICLE_NUMBER", "DATE_BREAK")) # Sort by vehicle number and date of maintenance
trimet_gaps <- distinct(trimet_gaps, VEHICLE_NUMBER, .keep_all= TRUE)

names(trimet_gaps)[names(trimet_gaps)=='DATE'] <- 'DATE_MAINT'
trimet_gaps <- subset(trimet_gaps, select=c(VEHICLE_NUMBER, DATE_MAINT, DATE_BREAK))

trimet_gaps_all <- merge(trimet_gaps_all, trimet_gaps, by='VEHICLE_NUMBER') 
trimet_gaps_all <- filter(trimet_gaps_all, DATE>DATE_MAINT & DATE<DATE_BREAK)
trimet_ngaps <- aggregate(MAINT_N ~ VEHICLE_NUMBER, data = trimet_gaps_all, sum) # Aggregate maintenance #

trimet_days <- merge(trimet_days, trimet_gaps, by='VEHICLE_NUMBER') # Merge with all operations days
trimet_days_break <- filter(trimet_days, DATE>=DATE_MAINT & DATE<=DATE_BREAK) # Keep only operations between maintenance and break
