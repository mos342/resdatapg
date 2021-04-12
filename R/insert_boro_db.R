#' Add borough-level data to database
#'
#' @param dat A data.frame with correct columns for the boro table and at least one row
#'
#' @param log Location of log file. Default is print to console
#'
#' @return
#' Logical - TRUE for worked ok.
#'
#' @export
#' @examples
#' dat<-data.frame(dataset='test',x=1, y=9, lad11nm="test")
#' insert_boro_db(dat)
#' @import readxl
#' @import RPostgres
#' @import DBI

insert_boro_db <- function(dat, log = "") {
    #dpth <- Sys.getenv("DASH_DATAPATH")
    dtst <- unique(dat$dataset)
    clmns <- c("dataset", "x", "y", "lad11nm")

    #column names in postgres must be lowercase
    names(dat) <- tolower(names(dat))

    ## throw error if all the columns are not there
    if(!all(clmns %in% names(dat))) {
        print(names(dat))
        stop("This is not going to work. You need all the columns")
    } else {
        ## remove columns we don't need
        dat <- dat[, clmns]
        ## make sure this is date
        dat$x <- as.Date(as.numeric(dat$x), origin = "1970-1-1")
    }
    cat(glue::glue("\n\n############ ADDING {dtst} ####################\n\n"),
        file = log, append = TRUE)

    conn <- dbConnect(
        Postgres(),
        #dbname = "dpadb",
        dbname = "monika",
        host = "localhost",
        port = 5432,
        password = Sys.getenv("password"),
        user = Sys.getenv("user")
    )

    if("ind_boro_dat" %in% dbListTables(conn)) {
        gsql <- glue::glue_sql("DELETE FROM ind_boro_dat WHERE dataset = {dtst}",
                         .con = conn)
        qry <- dbSendQuery(conn = conn, gsql)
        rws <- dbGetRowsAffected(qry)
        cat(glue::glue("Number of rows deleted: {rws}\n\n"), file = log, append = TRUE)
        dbClearResult(qry)
        dbAppendTable(conn, "ind_boro_dat", dat)
        cat(glue::glue("Number of rows added: {nrow(dat)}\n\n"),
            file = log, append = TRUE)
    } else {
        dbWriteTable(conn, "ind_boro_dat", dat)
        cat(glue::glue("Number of rows added: {nrow(dat)} TO NEW TABLE\n\n"),
            file = log, append = TRUE)
    }
    # dbCommit(conn)
    dbDisconnect(conn)
    TRUE
}

####
# Important to remember that for this data, a London-level data set needs to
# be created and stacked to the rest.
