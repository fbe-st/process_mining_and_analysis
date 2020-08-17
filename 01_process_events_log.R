library(tidyverse)
library(DBI)
library(RPostgres)
library(jsonlite)
library(bupaR)
library(heuristicsmineR)

# Create DB connection:
creds <- fromJSON("../sharktank_creds.json") # Replace with your own credentials in a JSON object with the relevant keys below:
con <- dbConnect(Postgres(), 
                 dbname = creds$DB_NAME, 
                 host = creds$DB_HOST, 
                 port = creds$DB_PORT, 
                 user = creds$DB_USER, 
                 password = creds$DB_PWD)

# Get activities remapping table:
activities_remapping <- readxl::read_excel("resources/unique_activities_remapping.xlsx", n_max = 88) %>% 
  select(unique_activity, recoded_activity) %>% 
  drop_na()

# Get User Behaviour Events (UBE) dataset from playpen:
ube_df <- dbGetQuery(con, "SELECT ube.user_id, client_name, event_name, event_timestamp, event_object_id, event_object_description, source, fur.user_role
                               FROM playpen.ub001_user_behaviour_events ube
                           LEFT JOIN core.filter_user_roles fur
                               ON ube.user_id = fur.user_id
                           WHERE client_name = 'weare'") # We're filtering for only weare data to intuit results from our familiarity with how we use the product. Remove this line to query for all UBE data.

# Get distinct list of pendo pages events:
pendo_events_mapping <- ube_df %>% 
  filter(source == "pendo_events") %>% 
  mutate(recoded_activity = paste0("events-", event_name)) %>% 
  mutate(recoded_activity = str_replace_all(tolower(recoded_activity), " ", "_")) %>% 
  mutate(unique_activity = paste0(event_object_description, "-", event_name)) %>% 
  select(unique_activity, recoded_activity) %>% 
  distinct() %>% 
  arrange(unique_activity)

# Add distinct pendo pages events to activities remapping table:
activities_remapping <- activities_remapping %>% 
  bind_rows(pendo_events_mapping)

# Create named vector (dict) for remapping events:
activity_mapper <- setNames(activities_remapping$recoded_activity, activities_remapping$unique_activity)

# Define user session threshold to 3,600 seconds:
session_time_threshold <- 60*60

# Transform UBE data to convert to event log object for applying process mining and analysis:
events_selection <- ube_df %>%
  mutate(activity = paste0(event_object_description, "-", event_name)) %>% # Create variable "activity" by concatenating "event_object_description" and "event_name" for remapping activities in next line.
  mutate(recoded_activity = activity_mapper[activity]) %>% # Remap activities using activity mapper created above.
  drop_na() %>% # Remove NA, particularly the rows with user_creation and last_login event.
  mutate_if(is.character, str_replace_all, pattern = "[',\\@\\(\\)#-]", replacement = "_") %>% # Remove problematic punctuation characters from all columns of type character/string.
  mutate(recoded_activity = trimws(recoded_activity, whitespace = "_")) %>% # Remove lagging underscores from replacement of problematic characters above.
  arrange(user_id, event_timestamp) %>% # Sort rows by user_id and then by event_timestamp.
  group_by(user_id) %>% 
  mutate(time_since_last = as.numeric(event_timestamp - lag(event_timestamp))) %>% # Measure duration in seconds between one event and the next.
  mutate(new_session = is.na(time_since_last) | time_since_last > session_time_threshold) %>% # Create flag variable marking if time_since_last is NA (i.e. the first event of a user), or if current event's time since last session is greater than the session threshold.
  mutate(session_id = cumsum(new_session)) %>% # Number the session_id as the cumulative sum of ones (TRUE values of flag column above).
  mutate(activity_repeat = recoded_activity == lag(recoded_activity)) %>% # Mark events which are a repeat of the previous event (e.g. when user modifies several tasks in a row).
  mutate(activity_repeat = replace_na(activity_repeat, FALSE)) %>% # Replace NA (i.e. the first event of a user) with the value "FALSE".
  relocate(activity_repeat, .after = recoded_activity) %>% # Reorganize the order of the columns, placing "activity_repeat" after "recoded_activity".
  ungroup(user_id) %>% 
  filter(activity_repeat == FALSE) %>% # Remove repeated events.
  select(-c(time_since_last, new_session)) %>% # Remove the columns "time_since_last" and "new_session".
  mutate(activity_instance_id = paste0(user_id, "_", recoded_activity, "_", session_id)) %>% # Create variable "activity_instance_id" by concatenating "user_id", "recoded_activity", "session_id" to use as "activity_instance_id" in events log object below.
  mutate(user_session_id = paste0(user_id, "_", session_id)) %>% # Create variable "user_session_id" by concatenating "user_id" and "session_id" to use as "case_id" in events log object below.
  arrange(user_id, event_timestamp) # Sort by user_id and event_timestamp once again. Should not be necessary, but doesn't hurt to be extra sure.


# Create Events Log Object: as per http://bupar.net/creating_eventlogs.html
events_selection_log <- events_selection %>% 
  select(user_id, activity_instance_id, user_session_id, session_id, recoded_activity, event_timestamp, event_object_description) %>% 
  mutate(status = "complete") %>% 
  eventlog(case_id = "user_session_id",
           activity_id = "recoded_activity", 
           activity_instance_id = "activity_instance_id", 
           lifecycle_id = "status", 
           timestamp = "event_timestamp", 
           resource_id = "event_object_description")

user_roles_df <- events_selection %>% 
  select(user_id, user_role) %>% 
  distinct()

events_selection_log <- events_selection_log %>% 
  left_join(user_roles_df, by = "user_id")

# Housecleaning:
rm(events_selection) 

# Get summary of events log:
events_selection_log %>% summary


##############################################
##### PROCESS MINING FOR ALL EVENTS WERE #####
##############################################

# Causal net between events, calculated using the precedence matrices from above, with dependencies' thresholds calculated using the Flexible Heuristics Miner approach:
causal_net(events_selection_log, threshold = .8) %>% 
  render_causal_net()

# Causal net between events, removing Pendo pages events:
causal_net(events_selection_log %>% filter(!grepl("events_", .$recoded_activity)), threshold = .8) %>% 
  render_causal_net()

# Causal net between events, for a single user (Francis in this example):
causal_net(events_selection_log %>% filter(user_id == "1be68175_97c3_41c3_b2b5_3050bb1eb57b"), threshold = .8) %>% 
  render_causal_net()

# Causal net between events, for a single user role:
causal_net(events_selection_log %>% filter(user_role == "pm"), threshold = .8) %>% 
  render_causal_net()

# Causal net between events, for a single user role:
causal_net(events_selection_log %>% filter(user_role == "stakeholder"), threshold = .8) %>% 
  render_causal_net()

#########################################
##### ANALYSIS FOR ALL EVENTS WEARE #####
#########################################

# Precedence Analysis. Given the large amount of distinct events, the tables are more informative and useful than the plots.
precedence_with_events_all <- precedence_matrix(events_selection_log, type = "absolute") # Calculate precedence matrix.
precedence_with_events_all %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precedence_with_events_all %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precedence_with_events_all %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the antecedent event.
precedence_with_events_all %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relevant ANTECEDENTS (i.e. "given antecendent A, it is followed x% of the time by Consequent B"):
precedence_rel_ant_with_events_all <- precedence_matrix(events_selection_log, type = "relative-antecedent") 
precedence_rel_ant_with_events_all %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precedence_rel_ant_with_events_all %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precedence_rel_ant_with_events_all %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the antecedent event.
precedence_rel_ant_with_events_all %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relevant CONSEQUENTS (i.e. " given consequent B, it is preceded x% of the time by Antecedent A"):
precendence_rel_con_with_events_all <- precedence_matrix(events_selection_log, type = "relative-consequent") 
precendence_rel_con_with_events_all %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precendence_rel_con_with_events_all %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precendence_rel_con_with_events_all %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where acce
precendence_rel_con_with_events_all %>% plot # View matrix viz. However, too dense due to excess distinct events.


#################################################################################
##### FILTER EVENTS  LOG JUST TO TRACES WHICH ACCESSED THE KB AT SOME POINT #####
#################################################################################

# Filter to only sessions which accessed the KB at some point:
log <- events_selection_log %>% 
  group_by_case() %>% 
  mutate(accessed_kb = any(recoded_activity == "help_access_kb")) %>% 
  ungroup_eventlog() %>% 
  filter(accessed_kb == TRUE)

# Housecleaning:
rm(events_selection_log)


############################
##### PROCESS ANALYSIS #####
############################ 

# Analyse sequences of events which end at the KB:
log %>% 
  filter_endpoints(start_activities = activities_remapping %>% 
                     select(recoded_activity) %>% 
                     filter(recoded_activity != "help_access_kb") %>% 
                     pull(), 
                   end_activities = "help_access_kb") %>%
  process_map()

# Analyse sequences of events which start at the KB:
log %>% 
  filter_endpoints(start_activities = "help_access_kb", end_activities = activities_remapping %>% 
                     select(recoded_activity) %>% 
                     filter(recoded_activity != "help_access_kb") %>% 
                     pull()) %>%
  process_map()

# Analyse sequences of events which contain accessing the KB:
# The top 5% of most common endpoint-pairs will be selected:
log %>% 
  filter_endpoints(percentage = 0.05) %>%
  process_map()

# Which activities were most present in sessions when users accessed the KB:
# Absolute event frequency:
log %>% 
  activity_frequency("activity") %>% 
  plot
# Relative event frequency:
log %>% 
  activity_presence() %>%
  plot

# Which activity is the most common starting action to traces which include accessing the KB:
start_act <- log %>% 
  start_activities("resource-activity") %>% 
  view("most_common_starts")

# Which activity is the most common ending action to traces which include accessing the KB:
end_act <- log %>% 
  end_activities("resource-activity") %>% 
  view("most_common_ends")

# relationship between the number of different activity sequences (i.e. traces) and the number of cases they cover:
log %>% 
  trace_coverage("trace") %>%
  plot() # Very few distinct traces

# length of traces, i.e. the number of activity instances for each case. 
log %>% 
  trace_length("case") %>%
  plot

# length of traces, i.e. the number of activity instances for each case. 
log %>% 
  trace_length("log") %>%
  plot

# compare summary of log dataset to summary of events dataset:
log %>% summary()

# Precedence Analysis. Given the large amount of distinct events, the tables are more informative and useful than the plots.
precedence_with_events <- precedence_matrix(log, type = "absolute") # Calculate precedence matrix.
precedence_with_events %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precedence_with_events %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precedence_with_events %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the antecedent event.
precedence_with_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis excluding Pendo pages events, i.e. only delivery unit, workitems, and help_button events:
precedence_wo_events <- precedence_matrix(log %>% filter(!grepl("events_", .$recoded_activity)), type = "absolute") 
precedence_wo_events %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precedence_wo_events %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precedence_wo_events %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where acce
precedence_wo_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relative values (otherwise ame as above):
precedence_rel_with_events <- precedence_matrix(log, type = "relative") 
precedence_rel_with_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relative values, excluding Pendo pages events:
precedence_rel_wo_events <- precedence_matrix(log %>% filter(!grepl("events_", .$recoded_activity)), type = "relative") 
precedence_rel_wo_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relevant ANTECEDENTS (i.e. "given antecendent A, it is followed x% of the time by Consequent B"):
precedence_rel_ant_with_events <- precedence_matrix(log, type = "relative-antecedent") 
precedence_rel_ant_with_events %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precedence_rel_ant_with_events %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precedence_rel_ant_with_events %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the antecedent event.
precedence_rel_ant_with_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relevant ANTECEDENTS, without Pendo pages events (i.e. "given antecendent A, it is followed x% of the time by Consequent B"):
precedence_rel_ant_wo_events <- precedence_matrix(log %>% filter(!grepl("events_", .$recoded_activity)), type = "relative-antecedent") 
precedence_rel_ant_wo_events %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precedence_rel_ant_wo_events %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precedence_rel_ant_wo_events %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where acce
precedence_rel_ant_wo_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relevant CONSEQUENTS (i.e. " given consequent B, it is preceded x% of the time by Antecedent A"):
precendence_rel_con_with_events <- precedence_matrix(log, type = "relative-consequent") 
precendence_rel_con_with_events %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precendence_rel_con_with_events %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precendence_rel_con_with_events %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where acce
precendence_rel_con_with_events %>% plot # View matrix viz. However, too dense due to excess distinct events.

# Precedence Analysis with relevant CONSEQUENTS, without Pendo pages events (i.e. " given consequent B, it is preceded x% of the time by Antecedent A"):
precendence_rel_con_wo_events <- precedence_matrix(log %>% filter(!grepl("events_", .$recoded_activity)), type = "relative-consequent") 
precendence_rel_con_wo_events %>% arrange(desc(n)) %>% view() # View all precedence relationships, sorted by largest frequency of precedence occurrence.
precendence_rel_con_wo_events %>% filter(consequent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where accessing the knowledge base was the consequent event.
precendence_rel_con_wo_events %>% filter(antecedent == "help_access_kb") %>% arrange(desc(n)) %>% view() # View all precedence relationships, where acce
precendence_rel_con_wo_events %>% plot # View matrix viz. However, too dense due to excess distinct events.


##########################
##### PROCESS MINING #####
##########################

# Causal net between events, calculated using the precedence matrices from above, with dependencies' thresholds calculated using the Flexible Heuristics Miner approach:
causal_net(log, threshold = .8) %>% 
  render_causal_net()

# Causal net between events, removing Pendo pages events:
causal_net(log %>% filter(!grepl("events_", .$recoded_activity)), threshold = .8) %>% 
  render_causal_net()

# Causal net between events, for a single user (Francis in this example):
causal_net(log %>% filter(user_id == "1be68175_97c3_41c3_b2b5_3050bb1eb57b"), threshold = .5) %>% 
  render_causal_net()

# Most frequent traces, with visualization and statistics on each trace. However, too dense given the number of distinct activity, and the long description names of each event.
log %>%
  trace_explorer()


# REDUNDANT ANALYSIS:
# Dependency matrix between events, calculated using the precedence matrices from above, with dependencies' thresholds calculated using the Flexible Heuristics Miner approach:
# dependency_matrix(log, threshold = .8) %>% 
#   render_dependency_matrix()
# 
# Dependency matrix between events, removing Pendo pages events:
# dependency_matrix(log %>% filter(!grepl("events_", .$recoded_activity)), threshold = .8) %>% 
#   render_dependency_matrix()
# 
# Visualising as Petri Nets:
# causal_net(log, threshold = .8) %>% 
#   as.petrinet() %>% 
#   render_PN()
# 
# log %>% 
#   filter(user_id == "1be68175_97c3_41c3_b2b5_3050bb1eb57b") %>% 
#   causal_net(threshold = .66) %>% 
#   as.petrinet() %>% 
#   render_PN()

renv::snapshot()
