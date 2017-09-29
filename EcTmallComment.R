

#清除環境變數
rm(list = ls())

#載入packages
library(rvest)
library(jsonlite)
library(DC0001.RMongo)
library(tmcn)
library(stringr)
setwd("/Volumes/Share/Data/外部數據-1號店/0921YhdComment")
web_all = list.files()
web_all = list.files()[-1]
m = rmongo()

for(i in 1:length(web_all)){
  web = read_html(web_all[i], encoding = "UTF-8")
  
  #product
  product = web %>% html_nodes("h1") %>% html_text()
  product = str_trim(product, side = "both")
  product = iconv(product)
  product = toTrad(product, rev=T)
  
  #postdate
  postDate = web %>% html_nodes(".date") %>% html_text()
  if(length(postDate) == 0) {postDate = NA}else{
    postDate = sapply(postDate, function(x) substr(x, nchar(x)-18,nchar(x)))
    postDate = gsub("[\\-]+", "", postDate)
    postDate = gsub("[\\:]+", "", postDate)
    postDate = paste0(substr(postDate, 1, 8), substr(postDate, 10, 15), "000")
    names(postDate) = NULL
  }
  
  #commentflavor
  commentFlavor = NA
  
  #comment
  comment = web %>% html_nodes(".btn") %>% html_text()
  if(length(comment) == 0) next
  comment = gsub("\n", "", comment)
  comment = str_trim(comment, side = "both")
  comment = iconv(comment)
  comment = toTrad(comment, rev = T)
  
  #starnumber
  starNumber = web %>% html_nodes(".star") %>% html_attr("class")
  starNumber = gsub("star s", "", starNumber)
  starNumber = as.integer(starNumber)
  
  #url
  url = web %>% html_nodes("head > link:nth-child(11)") %>% html_attr("href")
  url = paste0("https:", url)
  
  #productId
  productID = web %>% html_nodes("#experoienceProductId") %>% html_attr("value")
  
  #crawldate
  crawlDate = paste0(substr(web_all[i], regexpr("-",web_all[i])[1]+1,regexpr("-",web_all[i])[1]+8), "000000000")
  
  for(j in 1:length(comment)){
    cday = substr(crawlDate, 1, 8)
    if(is.na(postDate[1])){
      a = list("_id" = unbox(paste0("YHD_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday, 
                                       17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
               productID = unbox(productID),
               product = unbox(product),
               postDate = unbox(NA),
               commentID = unbox(paste0("YHD_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday, 
                                       17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
               commentFlavor = unbox(NA),
               comment = unbox(comment[j]),
               starNumber = unbox(as.integer(starNumber[j])),
               crawlDate = unbox(crawlDate),
               url = unbox(url),
               keywordList = list())
    }else{
      a = list("_id" = unbox(paste0("YHD_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday, 
                                 17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
               productID = unbox(productID),
               product = unbox(product),
               postDate = unbox(postDate[j]),
               commentID = unbox(paste0("YHD_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday, 
                                  17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
               commentFlavor = unbox(NA),
               comment = unbox(comment[j]),
               starNumber = unbox(as.integer(starNumber[j])),
               crawlDate = unbox(crawlDate),
               url = unbox(url),
               keywordList = list())
    }
    
    b = toJSON(a, always_decimal = T )
    key = m$find(
      query = toJSON(list(url = unbox(url[j]), comment = unbox(comment[j]), postDate = unbox(postDate[j]))),
      fields = '{"url": true, "comment": true, "postDate": true,"_id": false}'
    )
    if(dim(key)[1] == 0){
      m$insert(b)
    }
  }
}

##################################
#把comment的html資訊回塞第一層commentNumber/goodRate/flavorList/commentTagList
web_prod = web_all[which(grepl("0001.html", web_all))]
for(i in 1:length(web_prod)){
  web = read_html(web_prod[i], encoding = "UTF-8")
  
  #URL
  url = web %>% html_nodes("head > link:nth-child(11)") %>% html_attr("href")
  url = paste0("https:", url)
  
  #crawlDate
  crawlDate = paste0(substr(web_prod[i], regexpr("-",web_prod[i])[1]+1,regexpr("-",web_prod[i])[1]+8), "000000000")
  
  #goodRate
  goodRate = web %>% html_nodes("p span") %>% html_text() 
  goodRate = goodRate[which(grepl("[0-9]+", goodRate))][1]
  goodRate = as.numeric(goodRate)*0.01
  
  #commentTagList
  commentTagList = web %>% html_nodes("#div_labelSummary a") %>% html_text() 
  commentTagList = sapply(commentTagList, 
                          function(x) substr(x, 1, unlist(gregexpr("\\(", x))[1]-1))
  names(commentTagList) = NULL
  #commentNumber
  commentNumber = web %>% html_nodes("#all-comment_num") %>% html_text() 
  commentNumber = as.numeric(commentNumber)[1]
  
  #UPDATE
  if(length(goodRate) != 0 && !is.na(goodRate)){
    m$update(query = toJSON(list(url = unbox(url), crawlDate = unbox(crawlDate))),
             update = paste0('{"$set":{"goodRate":', format(goodRate, nsmall = 2),'}}'),
             multiple = T
    )
  }
  
  if(length(commentNumber) != 0 && !is.na(commentNumber)){
    m$update(query = toJSON(list(url = unbox(url), crawlDate = unbox(crawlDate))),
             update = paste0('{"$set":{"commentNumber":', commentNumber,'}}'),
             multiple = T
    )
  }
  
  for(j in 1:length(commentTagList)){
    m$update(query = toJSON(list(url = unbox(url), crawlDate = unbox(crawlDate))),
             update = paste0('{"$push":{"commentTagList":"', commentTagList[j],'"}}'),
             multiple = T
    )
  }
}

##################################
#把Product這層的keyword回塞comment
m = rmongo()
#Product的資訊
crawlDate = unlist(lapply(m$iterate()$batch(1192), function(x) x$crawlDate))
crawlnum = c(which(grepl("20170920",crawlDate)), which(grepl("20170921",crawlDate)))
crawlDate = crawlDate[crawlnum]

url = unlist(lapply(m$iterate()$batch(1192), function(x) x$url))
url = url[crawlnum]

keywordList = lapply(m$iterate()$batch(1192), function(x) x$keywordList)
keywordList = keywordList[crawlnum]

#回塞comment
for(i in 1:length(url)){
  for(j in 1:length(unlist(keywordList[i]))){
    m$update(query = toJSON(list(url = unbox(url[i]), crawlDate = unbox(crawlDate[i]))),
             update = paste0('{"$push":{"keywordList":"', unlist(keywordList[[i]][j]),'"}}'),
             multiple = T
    )
  }
}

