

#清除環境變數
rm(list = ls())

#載入packages
library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(tmcn)
library(DC0001.RMongo)

setwd("/Volumes/Share/Data/外部數據-1號店/Product")
web_all = list.files()
web_list = web_all[which(sapply(web_all, function(x) if(substr(x,regexpr("-", x)[1]+1, regexpr("-", x)[1]+8) == "20170921") TRUE else FALSE))]
web_list = web_list[which(sapply(web_list, function(x) if(substr(x,regexpr("-", x)[1]+10, regexpr("-", x)[1]+13) == "0001") TRUE else FALSE))]
m = rmongo()

for(j in 1:length(web_list)){
  web_1 = read_html(web_list[j], encoding = "UTF-8")

  #product
  product_1 = web_1 %>% html_nodes(".proName a:nth-child(1)") %>% html_text()
  product_1 = gsub("[\n\t]", "", product_1)
  product_1 = str_trim(product_1, side = "both")
  product_1 = iconv(product_1)
  product_1 = toTrad(product_1, rev=T)

  #price
  price_1 = web_1 %>% html_nodes(".num") %>% html_text()
  price_1 = as.numeric(gsub("¥","",price_1))
  price_1
  #shop
  shop = "1号店"
  #seller
  seller_1 = web_1 %>% html_nodes(".o_1") %>% html_text()
  seller_1 = gsub("[\n\t]", "", seller_1)
  seller_1 = str_trim(seller_1, side = "both")
  seller_1 = iconv(seller_1)
  seller_1 = toTrad(seller_1, rev=T)
  seller_1
  #keyword
  keyword = if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "baby"){
    "婴幼食品"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "candy"){
    "糖果"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "flavored"){
    "风味奶"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "gift"){
    "年货礼盒"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "milk"){
    "儿童奶"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "noodle"){
    "方便面"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "nut"){
    "坚果炒货"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "penghua"){
    "膨化食品"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "roll"){
    "卷心酥"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "yogurt"){
    "酸奶"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "wine"){
    "梅酒"
  }else if(substr(web_list[j], 1, regexpr("-", web_list[j])[1]-1) == "ice"){
    "棒棒冰"
  }
  #commentNumber
  commentNumber = NA
  #goodRate
  goodRate = NA
  #flavorList

  #commentTagList

  #crawlDate
  crawlDate = paste0(substr(web_list[j], regexpr("-",web_list[j])[1]+1,regexpr("-",web_list[j])[1]+8), "000000000")
  #productID
  productID_1 = web_1 %>% html_nodes(".proName a:nth-child(1)") %>% html_attr("id")
  productID_1 = substr(productID_1, 9, 16)
  productID_1
  #url
  url_1 = NULL
  for(i in 1:length(product_1)){
    number = c(1:length(product_1))
    url = paste0("https://club.yhd.com/review/", productID_1[i], ".html")
    url_1 = c(url_1, url)
  }
  url_1
  for(i in 1:length(product_1)){
    if(is.na(price_1[i])){a = list("_id" = list(url = unbox(as.character(url_1[i])),
                                                crawlDate = unbox(as.character(crawlDate))),
                                   productID = unbox(as.character(productID_1[i])),
                                   product = unbox(as.character(product_1[i])),
                                   price = unbox(NA),
                                   shop = unbox(as.character(shop)),
                                   seller = unbox(as.character(seller_1[i])),
                                   keywordList = as.character(keyword),
                                   commentNumber = unbox(NA),
                                   goodRate = unbox(NA),
                                   flavorList = list(),
                                   commentTagList = list(),
                                   crawlDate = unbox(as.character(crawlDate)),
                                   url = unbox(as.character(url_1[i])))
    }else{a = list("_id" = list(url = unbox(as.character(url_1[i])),
                                crawlDate = unbox(as.character(crawlDate))),
                   productID = unbox(as.character(productID_1[i])),
                   product = unbox(as.character(product_1[i])),
                   price = unbox(price_1[i]),
                   shop = unbox(as.character(shop)),
                   seller = unbox(as.character(seller_1[i])),
                   keywordList = as.character(keyword),
                   commentNumber = unbox(NA),
                   goodRate = unbox(NA),
                   flavorList = list(),
                   commentTagList = list(),
                   crawlDate = unbox(as.character(crawlDate)),
                   url = unbox(as.character(url_1[i])))}
    
    b = toJSON(a, always_decimal = T )
    key = m$find(
      query = toJSON(list(url = unbox(url_1[i]), crawlDate = unbox(crawlDate))),
      fields = '{"url": true, "crawlDate": true,"_id": false}'
    )
    if(dim(key)[1] == 0){
      m$insert(b)
    }else{
      m$update(query = toJSON(list(url = unbox(url_1[i]), crawlDate = unbox(crawlDate))), 
               update = paste0('{"$push":{"keywordList":"', keyword,'"}}')
      )
    }
  }

}
