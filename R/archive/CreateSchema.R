require(RPostgres) ;require(DBI); require(xlsx)
options(scipen = 999)

conSuper <- dbConnect( dbDriver("Postgres"),
                       dbname = Sys.getenv("dbname"),
                       host = Sys.getenv("host"),
                       port = Sys.getenv("port"),
                       password = Sys.getenv("password"),
                       user = Sys.getenv("user")
)   

dbGetInfo(conSuper)

#create schema info 
setwd("E:/project_folders/apps/db")
schema_info <- "Schema/DBSchema.xlsx"

tables <- c("ind_dat", "ind_boro_dat", "mtd")
for(i in 1:length(tables)) {
    file <- read.xlsx(
        Sys.getenv("schema_path"),
        sheetName = i
    )
    assign( tables[[i]], file )
    print(tables[[i]])
    
    schema <- paste0(apply(file, 1, paste0, collapse = " "), collapse = ",")
    
    dbGetQuery(conSuper, paste0("DROP TABLE IF EXISTS ", tables[[i]]))
    dbGetQuery(conSuper, paste0("CREATE TABLE ",tables[[i]],"(", schema,")"))
    
    # load existing database
    file <- read.csv(paste0("E:/project_folders/apps/db/tables/",tables[[i]], ".csv"))
    assign( tables[[i]], file )
    
    #convert int to date. recommend to unify notation for date columns
    if("xvardt" %in% colnames(file))
    {
        file$xvardt <- as.Date(file$xvardt, origin="1970-01-01")
    }
    
    if("x" %in% colnames(file))
    {
        file$x <- as.Date(file$x, origin="1970-01-01")
    }
    
    #Prepare query 
    #1. Remove apostrophes
    file <-  as.data.frame(sapply(file, function(x) gsub("'", "", x)))

    #2. Transform values for INSERT INTO query
    values <- paste0(apply(file, 1, function(x) paste0("('", paste0(x, collapse = "', '"), "')")), collapse = ", ")
    
    #3. Correct "NULL" notation
    values <- gsub("'NULL'", "NULL", values)
    values <- gsub("'NA'", "NULL", values)
    
    dbSendQuery(conSuper, paste0("INSERT INTO ",tables[[i]],  " VALUES ", values, ";"), n = Inf )    
}

dbListTables(conSuper)
test <- dbGetQuery(conSuper, "SELECT * FROM ind_boro_dat", n = Inf )

