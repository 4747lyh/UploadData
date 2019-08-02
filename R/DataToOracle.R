#' DataFrameToORC

#'

#' This function allows you to upload in oracle DB

#' @param JDBCClassPath must be inputted. Where is the path to the JAR file containing the driver?

#' @param Url Url must be inppted. What is the DB host, port, sid?

#' @param Id Id must be inppted. What is the DB connecting ID?

#' @param Pw Pw must be inppted. What is the DB connecting password?

#' @param Owner Owner must be inppted. Who created the table?

#' @param Table Table must be inppted. What is the table name?

#' @param Truncate default is TRUE. FALSE is that rows from uploading table does not delete.

#' @param DataOnR DataOnR must be inppted. What is the data name which uploading in the table?

#' @param NCore defalut is 1. How many use local computer cores? if your all core use, it use detectCores().

#' @param NumOnePack defalut is 500. upload rows number once. if you select NCore 8 and NumOnePack 1000, 8000 rows insert DB once.

#' @keywords dldbgud

#' @examples

#' DataFrameToORC(

#'  JDBCClassPath = "{ORACLE_HOME}\jdbc\lib\ojdbc8.jar"

#'  , Url = "jdbc:oracle:thin:@<db_ip>:<db_port>:<db_sid>"

#'  , Id = "LeeYuHyung"

#'  , Pw = "GoodMan"

#'  , Owner = "LYH"

#'  , Table = "TestTable"

#'  , Truncate = TRUE

#'  , DataOnR = "MyData"

#'  , NCore = 1

#'  , NumOnePack = 500

#')



DataFrameToORC = function (
  
  JDBCClassPath
  
  , Url
  
  , Id
  
  , Pw
  
  , Owner
  
  , Table
  
  , Truncate = TRUE
  
  , DataOnR
  
  , NCore = 1
  
  , NumOnePack = 500){
  
  #CheckParameter
    
  if(!exists("JDBCClassPath")) stop("DataFrame do not exist")
  if(!exists("Url")) stop("Url do not exist")
  if(!exists("Id")) stop("Id do not exist")
  if(!exists("Pw")) stop("Pw do not exist")
  if(!exists("Owner")) stop("Owner do not exist")
  if(!exists("Table")) stop("Table do not exist")
  if(!exists("DataOnR")) stop("DataOnR do not exist")
  
  #options
  
  list.of.packages <- c("plyr", "dplyr","data.table", "stringr","readr",
                        
                        "RJDBC", "rJava", "doParallel", "foreach")
  
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
  
  if(length(new.packages)) install.packages(new.packages)
  
  lapply(list.of.packages, library, character.only = TRUE)
  
  memory.limit(5000000)
  
  options(scipen = 999)

  
  DataOnR <- data.frame(DataOnR) %>% mutate_if(is.factor, as.character)
  
  DataOnR <- replace(DataOnR, is.na(DataOnR), "")
  
  #Assgin Seq by NumOnePack
  
  DataOnR$Seq <- c(rep(1:as.numeric(nrow(DataOnR) %/% NumOnePack), each = NumOnePack),
                   
                   rep(max(as.numeric(nrow(DataOnR) %/% NumOnePack)), each = as.numeric(nrow(DataOnR) %% NumOnePack)))
  
  
  #Connecting
  
  drv <- JDBC(driverClass = "oracle.jdbc.driver.OracleDriver", classPath = JDBCClassPath, "'")
  
  conn <- dbConnect(drv, URL, Id, Pw)
  
  
  #Matching Column
  
  TableName <- dbGetQuery(conn, paste("SELECT * FROM", Owner, ".", Table, "WHERE ROWNUM<=1"))
  
  DataOnR <- DataOnR[, colnames(TableName)[colnames(TableName) %in% colnames(DataOnR)]]
  
  if((ncol(DataOnR)==1 & colnames(DataOnR) %in% "Seq") | nrow(DataOnR)==0) stop("There is not data")
  
  #Clustering Setting
  
  cl <- makeCluster(NCore)
  
  clusterExport(cl, ls(), envir = environment())
  
  clusterEvalQ(cl, {
    
    options(scipen = 999)
    
    library(RJDBC)
    
    drv <- JDBC(driverClass = "oracle.jdbc.driver.OracleDriver", classPath = JDBCClassPath, "'")
    
    conn <- dbConnect(drv, URL, Id, Pw)
    
  }
  
  )
  
  registerDoParallel(cl)
  
  
  
  #Truncate Table
  
  if(Truncate) dbSendUpdate(conn, paste("TRUNCATE TABLE", paste(Owner, Table, sep=".")))
  
  
  
  #Writing Data func
  
  db_write_table <- function(conn, Owner, Table, SubSetData){
    
    batch <- apply(SubSetData, 1,
                   
                   FUN = function(x) paste0("'", trimws(x), "'", collapse = ",")) %>%
      
      paste("SELECT", ., "FROM DUAL UNION ALL", collapse = " ")
    
    batch <- str_sub(batch, 1, -11)
    
    query <- paste("INSERT INTO",
                   
                   paste0(paste(Owner, Table, sep="."),
                          
                          "(", paste(colnames(SubSetData), collapse = ","), ")"),
                   
                   "\n", batch)
    
    dbSendUpdate(conn, query)
    
  }
  
  
  
  #Start Insert
  
  Start = Sys.time()
  
  cat("\n Loop Start Times ", Start)
  
  pb <- txtProgressBar(min = 0, max = max(DataOnR$Seq), style=3)
  
  foreach(i = 1:max(DataOnR$Seq),
          
          .packages = list.of.packages,
          
          .inorder = FALSE,
          
          .noexport = "conn",
          
          .errorhandling = "stop") %dopar% {
         setTxtProgressBar(pb, i)
         SubSet <- DataOnR %>% filter(Seq == i) %>% select(-Seq)
         db_write_table(conn, Owner, Table, SubSetData)
          
        }
            
  
  #Close Connected DB
  
  clusterEvalQ(cl, {dbDisconnect(conn)})
  
  close(pb)

  stopCluster(cl)
  
  
  #Check Time
  
  cat(paste("\n Loop End Times", Sys.time()))
  
  Etime=as.numeric(trunc(difftime(Sys.time(), Start, units='secs')))
  
  cat("\n Loop Elapsed Times",paste(Etime %/% 3600, Etime %% 3600 %/% 60, Etime %% 3600 %% 60,sep=":"))
  
}





FileToORC = function(
  
  JDBCClassPath
  
  , Url
  
  , Id
  
  , Pw
  
  , Owner
  
  , Table
  
  , Truncate = TRUE
  
  , FileName
  
  , Seperator = '|'
  
  , DeleteFile = FALSE
  
  , NCore = 1
  
  , NumOnePack = 500) {
  
  #options
  
  list.of.packages <- c("plyr", "dplyr", "readr")
  
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
  
  if(length(new.packages)) install.packages(new.packages)
  
  lapply(list.of.packages, library, character.only = TRUE)
  
  memory.limit(5000000)
  
  options(scipen = 999)
  
  if(!file.exists(FileName)) stop("FileName do not exist")
  
  if(grepl(".rds|.txt|.csv", FileName)) stop("File seperator must be .rds or .txt or .csv at present")
  
  if (grepl(".rds", FileName))  DataOnR = readRDS(FileName)
  
  if (grepl(".txt|.csv", FileName))
      
      DataOnR = read_delim(FileName, delim = Seperator,
                           
                           locale = locale(encoding = guess_encoding(FileName)[1,1] %>% as.character))

  
  DataFrameToORC = function (
    
    JDBCClassPath = JDBCClassPath
    
    , Url = Url
    
    , Id = Url
    
    , Pw = Pw
    
    , Owner = Owner
    
    , Table = Table
    
    , Truncate = Truncate
    
    , DataOnR = DataOnR
    
    , NCore = NCore
    
    , NumOnePack = NumOnePack
    
  )
  
  if(DeleteFile) file.remove(FileName)
  
}
