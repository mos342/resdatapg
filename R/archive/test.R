file <- read.csv("E:/project_folders/apps/db/tables/mtd.csv")

#ind <- apply(X, 1, function(x) is.na(x))

#X <- X[ !ind, ]
file <-filter(file, !is.na(file$dataset))


dat <- data.frame(dataset=character(0),xwhich=numeric(0),xvarchar=character(0),xvardt=numeric(0), yval=numeric(0), yvllb=character(0), text=character(0))
dat <- data.frame(dataset='test',xwhich=0,xvarchar='test',xvardt=0, yval=0, yvllb='test', text='test')

#' dat<-data.frame(dataset='test',x=1, y=9, lad11nm="test")
#' insert_boro_db(dat)

#' dat <- data.frame(dataset='test',xwhich=0,xvarchar='test',xvardt=1, yval=0, yvllb='test', text='test')
#' insert_db(dat)

#' dat <- data.frame(dataset='test',xwhich=0,xvarchar='test',xvardt=1, yval=0, yvllb='test', text='test')
#' insert_db(dat)
#'
