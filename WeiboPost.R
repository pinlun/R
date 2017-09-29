#黃品綸
#weibo
#2017/07/21
rm(list = ls())
library(rvest)
library(dplyr)
library(jsonlite)
library(stringr)
library(DC0001.RMongo)

###########################################
####            關鍵字評論            ####
##########################################
m = rmongo()
setwd("/Volumes/Share/Data/外部數據-微博/產品-貝比瑪瑪")

#milklist = list.files()[which(substr(list.files(), 1, 6) == "GUAMIN")]
milklist = list.files()
milklist

weiboparse = function(keyword, webhtml){
  keyword = keyword
  web = read_html(webhtml)
  poster = web %>% html_nodes(".W_texta .W_texta .W_fb") %>% html_text() %>% str_trim(., side = "right")
  poster
  url = web %>% html_nodes(".W_texta .W_texta .W_fb") %>% html_attr("href")
  chanurl = sapply(url, function(x) regexpr("[\\?]", x)[1])
  url = mapply(function(x, y) paste0(substr(x,1,chanurl), "profile_ftype=1&is_all=1"), url, chanurl)
  comment = web %>% html_nodes(".W_texta .W_texta .comment_txt") %>% html_text() %>% gsub("[\n\t]", "", .) %>% str_trim(., side = "both")
  showAll = sapply(comment, function(x) if(grepl("展开全文", x)) return("N") else return("Y"))
  pandd = web %>% html_nodes(".W_texta .W_texta .W_textb") %>% html_text()
  pandd = pandd[-seq(2,length(poster)*2, 2)]
  pan = sapply(pandd, function(x) substr(x, 1, regexpr("自", x)[1] - 3))
  findate = function(x){
    if(substr(x, 1, 2) == "今天"){
      ymd = substr(webhtml,regexpr("-", webhtml)[1] + 1, 13)
      time = substr(x, regexpr(":", x)[1]-2, regexpr(":", x)[1]+2)
      time = gsub(":", "", time)
      return(paste0(ymd, time, "00000"))
    }else if(grepl("-",x)){
      y = substr(x, 1, regexpr("-", x)[1]-1)
      m = str_pad(substr(x, unlist(gregexpr("-", x))[1] +1, unlist(gregexpr("-", x))[2] -1), 2, side = "left", pad = "0")
      d = str_pad(substr(x, unlist(gregexpr("-", x))[2] +1, unlist(gregexpr(":", x))[1] -4), 2, side = "left", pad = "0")
      time = substr(x, regexpr(":", x)[1]-2, regexpr(":", x)[1]+2)
      time = gsub(":", "", time)
      return(paste0(y, m, d, time, "00000"))
    }else{
      y = "2017"
      m = str_pad(substr(x, 1, regexpr("月", x)[1]-1), 2, side = "left", pad = "0")
      d = str_pad(substr(x, regexpr("月", x)[1]+1, regexpr("日", x)[1]-1), 2, side = "left", pad = "0")
      time = substr(x, regexpr(":", x)[1]-2, regexpr(":", x)[1]+2)
      time = gsub(":", "", time)
      return(paste0(y, m, d, time, "00000"))
    }
  } 
  postdate = sapply(pan, function(x) findate(x))
  
  findevice = function(x){
    if(regexpr("自", x)[1] > 0){
      substr(x, regexpr("自", x)[1] + 2, nchar(x) - 1)
    }else{NULL}
  }
  device = unlist(sapply(pandd, function(x) findevice(x)))
  
  status = web %>% html_nodes(".W_texta .feed_action_row4 .S_line1") %>% html_text()
  
  numloc = function(x){
    if(regexpr("[0-9]+", x)[1] > 0){
      substr(x, regexpr("[0-9]+", x)[1], regexpr("[0-9]+", x)[1] + attr(regexpr("[0-9]+", x), "match.length"))
    }else{
      0
    }
  } 
  collectnum = as.integer(sapply(status[seq(1, length(poster)*4, 4)], function(x) numloc(x)))
  tranpnum = as.integer(sapply(status[seq(2, length(poster)*4, 4)], function(x) numloc(x)))
  commentnum = as.integer(sapply(status[seq(3, length(poster)*4, 4)], function(x) numloc(x)))
  likenum = as.integer(sapply(status[seq(4, length(poster)*4, 4)], function(x) if(x == "") 0 else x))
  
  crawldate = paste0(format(Sys.time(), "%Y%m%d%H%M%OS"), substr(format(Sys.time(), "%OS3"), 4, 6))
  ans = NULL
  for(i in 1:length(poster)){
    cday = substr(webhtml, regexpr("-",webhtml)[1]+1, regexpr("-",webhtml)[1]+8)
    post = list(
      "_id" = unbox(paste0("WEIBO_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday, 
                      17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
      postDate = unbox(str_pad(postdate[i], 17, side = "right", pad = "0")),
      commentID = unbox(paste0("WEIBO_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday, 
                      17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
      poster = unbox(poster[i]),
      keywordList = keyword,
      comment = unbox(comment[i]),
      showAll = unbox(showAll[i]),
      device = unbox(device[i]),
      collectionNumber = unbox(collectnum[i]),
      tranpostNumber = unbox(tranpnum[i]),
      commentNumber = unbox(commentnum[i]),
      likeNumber = unbox(likenum[i]),
      crawlDate = unbox(str_pad(cday, 17, side = "right", pad = "0")),
      url = unbox(url[i])
    )
    key = m$find(
      query = toJSON(list(poster = unbox(poster[i]), comment = unbox(comment[i]))),
      fields = '{"keywordList": true, "_id": false}'
    )
    if(dim(key)[1] == 0){
      m$insert(toJSON(post))
    }else{
      m$update(query = toJSON(list(poster = unbox(poster[i]), comment = unbox(comment[i]))), 
               update = paste0('{"$push":{"keywordList":"', post$keywordList,'"}}')
               )
    }
  }
}
sapply(milklist, function(x) weiboparse("贝比玛玛", x))
#m$drop()
#檢查postdate
postdate = lapply(m$iterate()$batch(11139), function(x) x$postDate)
pl = sapply(postdate, function(x) nchar(x))
poster = lapply(m$iterate()$batch(11139), function(x) x$poster)
which(pl != 17)


###########################################
####            客群評論              ####
##########################################
m = rmongo()

setwd("/Volumes/Share/Data/外部數據-微博/評論者_2")
commentlist = list.files()

skip_list = NULL
for(i in commentlist){
  web = read_html(i)
  #評論者
  real_user = web %>% html_nodes(".username") %>% html_text()
  #評論者發文資訊
  info = web %>% html_nodes("#Pl_Core_UserInfo__6 .W_fl") %>% html_text() %>% gsub("\t","",.) %>% gsub("\n","",.) %>% str_trim(., side = "both")
  
  #若無資訊則跳過
  if(length(real_user) == 0 | length(info) == 0){
    skip_list = c(skip_list, i)
    next
  }else{
    #嘗試多種css
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
    
    if(length(post_info) == 0){
      next
    }else{
      #計算每則評論的間距
      tnum = which(sapply(post_info, function(x) grepl("ñ", x)))
      postperiod = c(tnum[2:length(tnum)], NA) - tnum
      postperiod = postperiod[-length(postperiod)]
      postperiod = c(tnum[1], postperiod)
      names(postperiod) = NULL
      #剔除非評論者的評論
      postperiod = postperiod[which(sapply(post_info[c(1, tnum[-length(tnum)] +1)], function(x) grepl(real_user, x)))]
      
      #判斷發文時間
      #根據不同型態抓取年月日
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
      #若發文時間與發文裝置同一筆資料，則割除
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
      
      #抓取html檔名的時間
      crawldate = str_pad(substr(i, regexpr("-", i)[1]+1, regexpr("-", i)[1]+8), 17, side = "right", pad = "0")
      
      #抓取發文裝置
      finddev = function(x, y){
        if(y == 7){
          if(grepl("來自", post_info[x +1])){
            substr(post_info[x +1], unlist(gregexpr("來自", post_info[x +1]))[1] +2, nchar(post_info[x +1]))
          }else{
            NA
          }
        }else{
          if(grepl("來自", post_info[x +1])){
            substr(post_info[x +1], unlist(gregexpr("來自", post_info[x +1]))[1] +2, nchar(post_info[x +1]))
          }else{
            post_info[x+2]
          }
        }
      }
      device = mapply(function(x, y) finddev(x, y), which(post_info %in% real_user), postperiod)
      
      #抓取收藏數
      catchnum = function(x){
        if(regexpr("[0-9]+", post_info[x])[1] > 0){
          substr(post_info[x], regexpr("[0-9]+", post_info[x])[1], regexpr("[0-9]+", post_info[x])[1] + attr(regexpr("[0-9]+", post_info[x]), "match.length"))
        }else{
          0
        }
      }
      collectnum = as.integer(sapply(tnum-3, function(x) catchnum(x)))
      collectnum = collectnum[which(sapply(post_info[c(1, tnum[-length(tnum)] +1)], function(x) grepl(real_user, x)))]
      
      #抓取轉發數
      tranpnum = as.integer(sapply(tnum-2, function(x) catchnum(x)))
      tranpnum = tranpnum[which(sapply(post_info[c(1, tnum[-length(tnum)] +1)], function(x) grepl(real_user, x)))]
      
      #抓取評論數
      commentnum = as.integer(sapply(tnum-1, function(x) catchnum(x)))
      commentnum = commentnum[which(sapply(post_info[c(1, tnum[-length(tnum)] +1)], function(x) grepl(real_user, x)))]
      
      #抓取讚數
      likenum = as.integer(sapply(tnum, function(x) catchnum(x)))
      likenum = likenum[which(sapply(post_info[c(1, tnum[-length(tnum)] +1)], function(x) grepl(real_user, x)))]
      
      #抓取評論
      commloc = function(x, y){
        if(y == 7){
          post_info[x+2]
        }else{
          if(grepl("來自", post_info[x+1])){
            paste0(post_info[(x+2):(x+y-5)], collapse = "")
          }else{
            post_info[x+3]
          }
        }
      }
      comment = mapply(function(x, y) commloc(x, y), which(post_info %in% real_user), postperiod)
      #比對有無顯示全文
      showAll = sapply(comment, function(x) if(grepl("展开全文", x)) return("N") else return("Y"))
      names(showAll) = NULL
      ans = NULL
      for(j in 1:length(comment)){
        cday = substr(i, regexpr("-", i)[1]+1, regexpr("-", i)[1]+8)
        post = list(
          "_id" = unbox(paste0("WEIBO_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday,
                                                                                                             17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
          postDate = unbox(str_pad(postdate[j], 17, side = "right", pad = "0")),
          commentID = unbox(paste0("WEIBO_", cday, str_pad(m$count(query = toJSON(list(crawlDate = unbox(str_pad(cday,
                                                                                                                 17, side = "right", pad = "0")))))+1, 6, side = "left", pad = "0"))),
          poster = unbox(real_user),
          keywordList = list(),
          comment = unbox(comment[j]),
          showAll = unbox(showAll[j]),
          device = unbox(device[j]),
          collectionNumber = unbox(if(is.na(collectnum[j])){NA}else{collectnum[j]}),
          tranpostNumber = unbox(if(is.na(tranpnum[j])){NA}else{tranpnum[j]}),
          commentNumber = unbox(if(is.na(commentnum[j])){NA}else{commentnum[j]}),
          likeNumber = unbox(if(is.na(likenum[j])){NA}else{likenum[j]}),
          crawlDate = unbox(str_pad(cday, 17, side = "right", pad = "0")),
          url = unbox(NA)
        )
        key = m$find(
          query = toJSON(list(poster = unbox(real_user), comment = unbox(comment[j]))),
          fields = '{"keywordList": true, "_id": false}'
        )
        if(dim(key)[1] == 0){
          m$insert(toJSON(post))
        }
      }
    }
  }
}

############################################
##              回塞url                  ##
###########################################
m = rmongo()
key1 = m$find(
  fields = '{"poster": true, "url": true, "_id": false}'
)
key1 = key1[!is.na(key1$url), ]

for(i in key$poster){
  m$update(query = toJSON(list(poster = unbox(i))),
           update = paste0('{"$set":{"url":"', key[which(key$poster == i), "url"][1],'"}}'),
           multiple = T
           )
}
#沒有網址的
m = rmongo()
#mongo original user name
key2 = m$find(
  fields = '{"poster": true, "url": true, "_id": false}'
)
key2 = unique(key2)
nakey = key2[is.na(key2$url), ]
nonakey = key2[!is.na(key2$url), ]

setwd("/Volumes/Share/Data/外部數據-微博/評論者_2")
commentlist = list.files()
#change name
ttl = NULL
for(i in commentlist){
  web = read_html(i)
  real_user = web %>% html_nodes(".username") %>% html_text()
  if(length(real_user) != 0){
    url = web %>% html_nodes("a.W_f14.W_fb.S_txt1") %>% html_attr("href")
    url = url[1]
    url = paste0("http://", substr(url, 3, regexpr("[\\?]", url)[1]), "profile_ftype=1&is_all=1")
    tmp = data.frame(i, real_user, url, stringsAsFactors = F)
    ttl = rbind(ttl, tmp)
  }else{
    next
  }
}

nakey$poster
for(i in nakey$poster){
  tmp = ttl[which(ttl$real_user == i), "url"][1]
  if(length(tmp) == 0){
    next
  }else{
    m$update(query = toJSON(list(poster = unbox(i))),
             update = paste0('{"$set":{"url":"', ttl[which(ttl$real_user == i), "url"][1],'"}}'),
             multiple = T
    )
  }
}
