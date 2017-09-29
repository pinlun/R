#黃品綸
#weibo
#2017/07/31
rm(list = ls())
library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(DC0001.RMongo)
m = rmongo()

setwd("/Volumes/Share/Data/外部數據-微博/評論者_2")
commentlist = list.files()

skip_list = NULL
for(i in commentlist){
  #客群樣貌
  web = read_html(i)
  real_user = web %>% html_nodes(".username") %>% html_text()
  info = web %>% html_nodes("#Pl_Core_UserInfo__6 .W_fl") %>% html_text() %>% gsub("\t","",.) %>% gsub("\n","",.) %>% str_trim(., side = "both")
  #資料量不夠就跳過
  if(length(real_user) == 0 | length(info) == 0){
    skip_list = c(skip_list, i)
    next
  }else{
    #評論者資訊
    birthday = if(length(which(info == "ö")) == 0) NA else info[which(info == "ö")+1]
    birloc = function(x){
      if(grepl("年", x)){
        y = substr(x, 1, unlist(gregexpr("年", x))[1]-1)
        m = str_pad(substr(x, unlist(gregexpr("年", x))[1]+1, unlist(gregexpr("月", x))[1]-1), width = 2, pad = "0")
        d = str_pad(substr(x, unlist(gregexpr("月", x))[1]+1, unlist(gregexpr("日", x))[1]-1), width = 2, pad = "0")
        return(paste0(y, m, d))
      }else{
        NA
      }
    }
    birthday = birloc(birthday)
    gendermark = web %>% html_nodes(".current .t_link") %>% html_text() %>% substr(., 1, 1)
    gender = if(gendermark == "她") "F" else "M"
    #關注/粉絲/發文數
    Numbers = web %>% html_nodes(".W_f18") %>% html_text()
    if(length(Numbers) == 0){
      Numbers = web %>% html_nodes(".W_f16") %>% html_text()
    }
    if(length(Numbers) == 0 | length(Numbers) < 3){
      followNumber = NA
      fansNumber = NA
      postNumber = NA
    }else{
      followNumber = as.integer(Numbers[1])
      fansNumber = as.integer(Numbers[2])
      postNumber = as.integer(Numbers[3])
    }
    location = if(length(which(info == "2")) == 0) NA else info[which(info == "2")+1]
    #發文全貌
    post_info = web %>% html_nodes(".pos .S_line1 , .WB_info+ .S_txt2 .S_txt2 , #Pl_Official_MyProfileFeed__21 .W_f14") %>% html_text() %>% gsub("[\n\t]", "", .) %>% str_trim(., side = "both")
    if(post_info[1] != real_user | length(post_info) == 0){
      post_info = web %>% html_nodes(".WB_info+ .S_txt2 .S_txt2 , .pos .S_line1 , #Pl_Official_MyProfileFeed__22 .W_f14") %>% html_text() %>% gsub("[\n\t]", "", .) %>% str_trim(., side = "both")
    }
    if(post_info[1] != real_user | length(post_info) == 0){
      post_info = web %>% html_nodes(".pos .S_line1 , #Pl_Official_MyProfileFeed__20 .W_f14 , .WB_info+ .S_txt2") %>% html_text() %>% gsub("[\n\t]", "", .) %>% str_trim(., side = "both")
    }
    if(post_info[1] != real_user | length(post_info) == 0){
      post_info = web %>% html_nodes(".pos .S_line1 , #Pl_Official_MyProfileFeed__23 .W_f14 , .WB_info+ .S_txt2") %>% html_text() %>% gsub("[\n\t]", "", .) %>% str_trim(., side = "both")
    }
    if(post_info[1] != real_user | length(post_info) == 0){
      post_info = character(0)
    }
    #擷取發文日期
    findate = function(x){
      if(grepl("月", substr(x, 1, 4))){
        if(nchar(substr(x, 1, regexpr("月", x)[1] - 1)) > 1){
          m = substr(x, 1, 2)
        }else{
          m = paste0("0", substr(x, 1, 1))
        }

        if(nchar(substr(x, regexpr("月", x)[1] +1, regexpr("日", x)[1] - 1)) > 1){
          d = substr(x, 3, 4)
        }else{
          d = paste0("0", substr(x, 3, 3))
        }
        time = substr(x, regexpr(":", x)[1] -2, nchar(x))
        time = gsub(":", "", time)
        return(paste0("2017", m , d, time, "00000"))
      }else if(grepl("-", x)){
        y = substr(x, 1, 4)
        m = str_pad(substr(x, unlist(gregexpr("-", x))[1]+1, unlist(gregexpr("-", x))[2]-1), 2, side = "left", pad = "0")
        d = str_pad(substr(x, unlist(gregexpr("-", x))[2]+1, unlist(gregexpr(":", x))[1]-4), 2, side = "left", pad = "0")
        time = substr(x, regexpr(":", x)[1] -2, nchar(x))
        time = gsub(":", "", time)
        return(paste0(y, m , d, time, "00000"))
      }else{
        ymd = substr(i,regexpr("-", i)[1] + 1, 13)
        time = substr(x, regexpr(":", x)[1]-2, regexpr(":", x)[1]+2)
        time = gsub(":", "", time)
        return(paste0(ymd, time, "00000"))
      }
    }
    #擷取字段有來自之前的時間
    cutdate = function(x){
      if(grepl("來自", x)){
        substr(x, 1, regexpr("來自", x)[1]-1)
      }else{x}
    }
    if(length(post_info) == 0){
      postdate = NA
    }else{
      tt = post_info[which(post_info == real_user)+1]
      tt = sapply(tt, function(x) cutdate(x))
      postdate = sapply(tt, function(x) findate(x))
      names(postdate) = NULL
    }
    #爬文時間
    crawldate = str_pad(substr(i, regexpr("-", i)[1]+1, regexpr("-", i)[1]+8), 17, side = "right", pad = "0")
    #list
    poster_list = list("_id" = unbox(real_user),
                       postDate = unbox(postdate[length(postdate)]),
                       poster = unbox(real_user),
                       birthday = unbox(birthday),
                       gender = unbox(gender),
                       followNumber = unbox(followNumber),
                       fansNumber = unbox(fansNumber),
                       postNumber = unbox(postNumber),
                       location = unbox(location),
                       crawldate = unbox(crawldate),
                       url = unbox(NA))
    #判斷mongo裡是否有此評論者
    #無則塞進去/有則跳過
    key = m$find(
      query = toJSON(list(poster = unbox(real_user))),
      fields = '{"poster": true, "_id": false}'
    )
    if(dim(key)[1] == 0){
      m$insert(toJSON(poster_list))
    }
  }
}

#判斷發文時間是否有超過17碼
postdate = lapply(m$iterate()$batch(11139), function(x) x$postDate)
pl = sapply(postdate, function(x) nchar(x))
poster = lapply(m$iterate()$batch(11139), function(x) x$poster)
which(pl != 17)

############################################
##              回塞url                  ##
###########################################
m = rmongo()
key1 = m$find(
  fields = '{"poster": true, "url": true, "_id": false}'
)
key1 = key1[!is.na(key1$url), ]

for(i in key1$poster){
  m$update(query = toJSON(list(poster = unbox(i))),
           update = paste0('{"$set":{"url":"', key1[which(key1$poster == i), "url"][1],'"}}'),
           multiple = T
  )
}
