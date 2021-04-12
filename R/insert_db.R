#' Add data to database
#'
#' @param dat A data.frame with correct columns and at least one row
#'
#' @param log Location of log file. Default is print to console
#'
#' @return
#' Logical - TRUE for worked OK.
#' @export
#'
#' @examples
#' dat <- data.frame(dataset='test',xwhich=0,xvarchar='test',xvardt=1, yval=0, yvllb='test', text='test')
#' insert_db(dat)
#' @import readxl
#' @import RPostgres
#' @import DBI
#' @import dplyr

insert_db <- function(dat, log = "") {
    clmns <- c("dataset", "xwhich", "xvarchar", "xvardt", "yval", "yvllb", "text")

    if(!all(clmns %in% names(dat))) {
        print(names(dat))
        stop("This is not going to work. You need all the columns")
    } else {
        dat <- dat[, clmns]
    }

    dtst <- unique(dat$dataset)
    cat(glue::glue("\n\n############ ADDING {dtst} ####################\n\n"),
        file = log, append = TRUE)
    if(dat$xwhich[1] == 2) {
        dat$xvardt <- as.Date(dat$xvardt, origin="1970-01-01")
        mindt <- format(min(dat$xvardt), "%d %b '%y")
        maxdt <- format(max(dat$xvardt), "%d %b '%y")
        xmsg <- glue::glue("Xaxis Date. Range:{mindt} - {maxdt}\n\n")
    } else {
        xmsg <- glue::glue("Xaxis Character: {toString(unique(dat$xvarchar))}\n\n")
    }

    conn <- dbConnect(
        Postgres(),
        #dbname = "dpadb",
        dbname = "monika",
        host = "localhost",
        port = 5432,
        password = Sys.getenv("password"),
        user = Sys.getenv("user")
    )

    m <- tbl(conn, "mtd") %>%
        filter(dataset == dtst) %>% dplyr::collect()

    if(nrow(m) == 0) {
        cat("NO METADATA ASSOCIATED WITH THIS DATA\n\n", file = log)

    } else {
        cat(m$charttitle, "\n\n", file = log, append = TRUE)

    cat(glue::glue("Y range: {min(dat$yval)} - {max(dat$yval)}\n\n"),
        file = log, append = TRUE)
    cat(xmsg, file = log, append = TRUE)

    if("ind_dat" %in% dbListTables(conn)) {
        gsql <- glue::glue_sql("DELETE FROM ind_dat WHERE dataset = {dtst}",
                         .con = conn)
        qry <- dbSendQuery(conn = conn, gsql)
        rws <- dbGetRowsAffected(qry)
        cat(glue::glue("Number of rows deleted: {rws}\n\n"), file = log, append = TRUE)
        dbClearResult(qry)
        dbAppendTable(conn, "ind_dat", dat)
        cat(glue::glue("Number of rows added: {nrow(dat)}\n\n"),
            file = log, append = TRUE)
    } else {
        dbWriteTable(conn, "ind_dat", dat)
        print(glue::glue("Number of rows added: {nrow(dat)}\n\n"), file = log,
              append = TRUE)
    }
    }
    # dbCommit(conn)
    dbDisconnect(conn)
    TRUE
}

#unempl <- read.csv("E:/project_folders/apps/db/updates/unemployment.csv")
#insert_db(unempl, log = "")


