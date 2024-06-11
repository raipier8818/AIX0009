library('stringr')
library('glmnet')

extract <- function(o, s) { 
  index <- which(coef(o, s) != 0) 
  data.frame(name=rownames(coef(o))[index], coef=coef(o, s)[index]) 
}

options(scipen=999)

args<-commandArgs(TRUE)


filtered = paste(args[1],args[2],args[3],args[4],'filtered-5-2',args[5],sep="-")
model_file = paste(args[2],args[3],args[4],args[5],'lasso-5s-2std',sep='-')

filtered <- str_remove_all(filtered,":")
model_file <- str_remove_all(model_file,":")

filtered = paste ("./", filtered, ".csv", sep="")
message(filtered)
message(model_file)
model_file = paste ("./", model_file, ".csv", sep="")

filtered = read.csv(filtered)
mid_std = sd(filtered$mid_price)
message (round(mid_std,0))
#print (round(mid_std[1],0))
#print (mid_std)

filtered_no_time_mid = subset(filtered, select=-c(mid_price,timestamp))

y = filtered_no_time_mid$return
x = subset(filtered_no_time_mid, select=-c(return))

#quit()

x<-as.matrix(x)
#model_ols<-lm(y~x)
# model_lasso<-glmnet(x,y)

# cv_fit <- cv.glmnet(x=x, y=y, alpha=0, intercept=FALSE, lower.limits=0, nfolds=10) #ridge
cv_fit <- cv.glmnet(x=x, y=y, alpha=1, intercept=FALSE, lower.limits=0, nfolds=5) #lasso

fit <- glmnet(x=x, y=y, alpha = 1, lambda=cv_fit$lambda.1se, intercept=FALSE, lower.limits=0,)
#coef(fit)

df <- extract(fit, s=0.1)
df <- t(df)
write.table(df, file=model_file, sep=",", col.names=FALSE, row.names=FALSE, quote=FALSE)