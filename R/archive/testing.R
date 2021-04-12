unempl <- read.csv("E:/project_folders/apps/db/updates/unemployment.csv")
insert_db(unempl, log = "")

clmts <- read.csv("E:/project_folders/apps/db/updates/claimant_count.csv")
insert_boro_db(clmts, log = "")

create_schema(tables = c("mtd", "ind_dat", "ind_boro_dat"))
create_schema(tables = c("mtd"))
