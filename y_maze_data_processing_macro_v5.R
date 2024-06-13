 ##testing modification for arm entries 
## changed ID location in metadata to find ID version 4 always location 35 (if this changes need to fix)
##line 55 and changed MNBF to NBP lines 62  and 323
#changed such that if number of triplets negative then = 0

insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}

options(warn=1)

# setwd(file.location)

args = commandArgs(trailingOnly=TRUE)

output.file<-args[2] #paste0("MNBF_Y-MAZE_SponAlt"," ","2020Jul1_SponAlt_Val_ZB",".csv")

ptm <- proc.time()

if (length(args)==0) {
  stop("At least one argument must be supplied (input file).n", call.=FALSE)
} else {
  files = unlist(strsplit(args[1],","))
}

library(readxl)
# files = list.files(pattern="*.xlsx")
Ymaze<-NULL
aa<-as.numeric(length(files))
#v3 change need to be as numeric for loop

for(a in 1:aa){
  num.sheet<-as.numeric(length(excel_sheets(files[a])))
  #v3 change need to be as numeric for loop
  sheet.name<-excel_sheets(files[a])
  for(b in 1:num.sheet){ 
    ############################################################
    #meta data information
    trialnum<-suppressMessages(read_excel(files[a],sheet=b,range = cell_rows(1:1),col_names = FALSE, na="-"))
    num.row<-as.numeric(as.character(trialnum[1,2]))-1
    
    
    meta.data<-suppressMessages(read_excel(files[a],sheet=b,range = cell_rows(1:num.row),col_names = FALSE, na="-"))
    meta.data<-data.frame(meta.data)
    
    ###new version of R changes no name to ...1 
    ####so when searching for char need to search metadata$...1  not
    ### as old version used metadata$X__1
    trial.row<-match("Trial name", as.character(meta.data$...1))
    arena.row<-match("Arena name", as.character(meta.data$...1))
    date.row<-match("Start time", as.character(meta.data$...1))
    
   # subject.row<-match("mouse id", tolower(as.character(meta.data$...1)))
    subject.row <- 35L
    ## changed ID location in metadata to find ID version 4 always location 35 (if this changes need to fix)
    
    Trial.Number<-as.character(meta.data[trial.row,2])
    Arena<-as.character(meta.data[arena.row,2])
   # MNBF.ID<-as.numeric(as.character(meta.data[subject.row,2]))
    ################################################################################################################################
    NBP.ID<-as.character(meta.data[subject.row,2])
    #changed name of ID variable version 4 and changed to char not number only
    
    Date<-unlist(strsplit(as.character(meta.data[date.row,2])," "))[1]
    Time<-unlist(strsplit(unlist(strsplit(as.character(meta.data[date.row,2])," "))[2],"[.]"))[1]
    
    #read data and refind column names
    dat<-read_excel(files[a],sheet=b,skip = num.row-1, na="-")
    #changed to num.row -1 to get only the data V3
    dat<-data.frame(dat)
    dat<- dat[-c(1),]
    #removed the unit line so column headers match V3
    
    #colnames(dat)[2]<-"s"
    
    coldat<-colnames(data.frame(read_excel(files[a],sheet=b,skip = num.row-1,na="-")))
    if(sheet.name[b]=="Track-Arena 1-Subject 1"){
     # if(is.na(match("In.zone.A1...Center.point.",coldat))){
       # colnames(dat)[match("In.zone.1a...Center.point.",coldat)]<-"A"
        #colnames(dat)[match("In.zone.1b...Center.point.",coldat)]<-"B"
        #colnames(dat)[match("In.zone.1c...Center.point.",coldat)]<-"C"
      #}else{
        colnames(dat)[match("In.zone.A1...Center.point.",coldat)]<-"A"
        colnames(dat)[match("In.zone.B1...Center.point.",coldat)]<-"B"
        colnames(dat)[match("In.zone.C1...Center.point.",coldat)]<-"C"
        #upper case Center.point for V3
    #  }
      
    }else{
     # if(is.na(match("In.zone.A2...center.point.",coldat))){
       # colnames(dat)[match("In.zone.2a...Center.point.",coldat)]<-"A"
       # colnames(dat)[match("In.zone.2b...Center.point.",coldat)]<-"B"
      #  colnames(dat)[match("In.zone.2c...Center.point.",coldat)]<-"C"
      #}else{
        colnames(dat)[match("In.zone.A2...Center.point.",coldat)]<-"A"
        colnames(dat)[match("In.zone.B2...Center.point.",coldat)]<-"B"
        colnames(dat)[match("In.zone.C2...Center.point.",coldat)]<-"C"
     # }
    }
    
    #Convert misdetected entries to 0
    dat[which(is.na(dat$A)),]$A<-rep(0,dim(dat[which(is.na(dat$A)),])[1])
    dat[which(is.na(dat$B)),]$B<-rep(0,dim(dat[which(is.na(dat$B)),])[1])
    dat[which(is.na(dat$C)),]$C<-rep(0,dim(dat[which(is.na(dat$C)),])[1])
    
    dat[which(dat$A=="-"),]$A<-rep(0,dim(dat[which(dat$A=="-"),])[1])
    dat[which(dat$B=="-"),]$B<-rep(0,dim(dat[which(dat$B=="-"),])[1])
    dat[which(dat$C=="-"),]$C<-rep(0,dim(dat[which(dat$C=="-"),])[1])
    
    dat$A<-as.numeric(as.character(dat$A))
    dat$B<-as.numeric(as.character(dat$B))
    dat$C<-as.numeric(as.character(dat$C))
    
    #Deal with non-A arm starters
    cnum1<-match("A",colnames(dat))
    cnum2<-match("B",colnames(dat))
    cnum3<-match("C",colnames(dat))
    
    if(sum(dat[1,c(cnum1,cnum2,cnum3)])==0){
      dat<-insertRow(dat,dat[1,],1)
      dat$A[1]<-1
    }
    if(dat$A[1]==0){
      dat<-insertRow(dat,dat[1,],1)
      dat[1,c(cnum1,cnum2,cnum3)]<-c(1,0,0)
    }
    
    #frame jump
    insert<-NULL
    for(fj in 1:(dim(dat)[1]-1)){
          se1<-match(1, dat[fj,c(cnum1,cnum2,cnum3)])
          se2<-match(1, dat[fj+1,c(cnum1,cnum2,cnum3)])
          se<-c(se1,se2)
          if(!TRUE%in%is.na(se) & se1!=se2){
               insert<-c(insert,fj)
          }
    }
    
    if(length(insert)>0){
      for(ir in 1:length(insert)){
        newrow<-dat[insert[ir]+ir,]
        newrow[,c(cnum1,cnum2,cnum3)]<-rep(0,3)
        dat<-insertRow(dat,newrow,insert[ir]+ir)
      }
    }
    
    code<-apply(dat[,c(cnum1,cnum2,cnum3)],1,sum)
    
    dat<-cbind(code,dat)
    
    row<-match(0,dat$code)
    
    colnames(dat)[2]<-"s"
    if(!is.na(row)){
      t1<-dat$s[1]
      t2<-dat$s[row]
      if(1%in%dat$A[1:(row-1)]){
        t3<-c("Start","End")
      }else{
        t3<-c(NA,NA)
      }
      if(1%in%dat$B[1:(row-1)]){
        t4<-c("Start","End")
      }else{
        t4<-c(NA,NA)
      }
      if(1%in%dat$C[1:(row-1)]){
        t5<-c("Start","End")
      }else{
        t5<-c(NA,NA)
      }
      temp<-cbind(c(t1,t2),t3,t4,t5)
      dat1<-temp
      
      while(row<dim(dat)[1] & !is.na(match(1,dat[-c(1:row),]$code))){
        num1<-match(1,dat[-c(1:row),]$code)
        num2<-row+num1
        num3<-match(0,dat[-c(1:num2),]$code)
        if(!is.na(num3)){
          row<-num2+num3
          t1<-dat$s[num2]
          t2<-dat$s[row]
          if(1%in%dat$A[num2:(row-1)]){
            t3<-c("Start","End")
          }else{
            t3<-c(NA,NA)
          }
          if(1%in%dat$B[num2:(row-1)]){
            t4<-c("Start","End")
          }else{
            t4<-c(NA,NA)
          }
          if(1%in%dat$C[num2:(row-1)]){
            t5<-c("Start","End")
          }else{
            t5<-c(NA,NA)
          }
          temp<-cbind(c(t1,t2),t3,t4,t5)
          dat1<-rbind(dat1,temp)
        }else{
          row<-dim(dat)[1]
          t1<-dat$s[num2]
          t2<-dat$s[row]
          if(1%in%dat$A[num2:row]){
            t3<-c("Start","End")
          }else{
            t3<-c(NA,NA)
          }
          if(1%in%dat$B[num2:row]){
            t4<-c("Start","End")
          }else{
            t4<-c(NA,NA)
          }
          if(1%in%dat$C[num2:row]){
            t5<-c("Start","End")
          }else{
            t5<-c(NA,NA)
          }
          temp<-cbind(c(t1,t2),t3,t4,t5)
          dat1<-rbind(dat1,temp)
        }
      }
    }else{
      t1<-dat$s[1]
      t2<-dat$s[dim(dat)[1]]
      if(1%in%dat$A){
        t3<-c("Start","End")
      }else{
        t3<-c(NA,NA)
      }
      if(1%in%dat$B){
        t4<-c("Start","End")
      }else{
        t4<-c(NA,NA)
      }
      if(1%in%dat$C){
        t5<-c("Start","End")
      }else{
        t5<-c(NA,NA)
      }
      temp<-cbind(c(t1,t2),t3,t4,t5)
      dat1<-temp
    }
    
    dat1<-data.frame(dat1)
    colnames(dat1)<-c("Time","A","B","C")
    dat1$Time<-as.numeric(as.character(dat1$Time))
    
    #time in each arm
    time1<-time2<-time3<-0
    j<-1
    while (j<=(dim(dat1)[1]-1)) {
      if(!is.na(dat1$A[j])){
        time1<-time1+dat1$Time[j+1]-dat1$Time[j]
      }
      if(!is.na(dat1$B[j])){
        time2<-time2+dat1$Time[j+1]-dat1$Time[j]
      }
      if(!is.na(dat1$C[j])){
        time3<-time3+dat1$Time[j+1]-dat1$Time[j]
      }
      j<-j+2
    }
    
    Sequence<-NULL
    TrialTime<-NULL
    ## local copy matches to "Start" but Galaxy matches to "2"
    
   # First<-c("A","B","C")[match("Start",as.character(dat1[1,-1]))]
    ######################################################################################################
    First<-c("A","B","C")[match("2",as.character(dat1[1,-1]))]
    
    Sequence<-c(Sequence,First)
    TrialTime<-c(TrialTime,dat1$Time[1])
    i<-3
    while(i<dim(dat1)[1]){
      
      entry<-c("A","B","C")[match("2",as.character(dat1[i,-1]))]
     # entry<-c("A","B","C")[match("Start",as.character(dat1[i,-1]))]
      ########################################################################################################
      
      if(!is.na(Sequence[length(Sequence)])) 
      {
	if(entry==Sequence[length(Sequence)])
        {
         lag<-dat1$Time[i]-dat1$Time[i-1]
         if(lag>1)
         {
           Sequence<-c(Sequence,entry)
           TrialTime<-c(TrialTime,dat1$Time[i])
         }
        }
        else
        {
          Sequence<-c(Sequence,entry)
          TrialTime<-c(TrialTime,dat1$Time[i])
        }
      }
      i<-i+2
    }
    
    Triad<-c("ABC","ACB","BAC","BCA","CAB","CBA")
    
    alt<-0
    for(i in 1:(length(Sequence)-2)){
      trial<-paste0(Sequence[i],Sequence[i+1],Sequence[i+2])
      if(trial%in%Triad){
        alt<-alt+1
      }
    }
    
    #parameters
    Latency.to.Leave.Start.Arm<-TrialTime[2]-TrialTime[1]
    Total.Arm.Entries<-length(Sequence)
    Number.of.Triplets<-Total.Arm.Entries-2
    ## if number of triplets negative then zero
    ###########################################################################################changed from 3
    if(Number.of.Triplets<0)
    Number.of.Triplets<-0
    
    
    Number.of.Spontaneous.Alternations<-alt
    Alternation.Ratio<-100*alt/(length(Sequence)-2)
    Entry.Sequence<-paste(Sequence,collapse = "")
    
  ##############modification to give all ABC entries  
    levels<-c("A","B","C")
    ABCtable<-table(factor(Sequence, levels, labels = levels,
                           exclude = NA, ordered = is.ordered(Sequence), nmax = NA))
    
    A.Arm.Entry<-ABCtable[1]
    B.Arm.Entry<-ABCtable[2]
    C.Arm.Entry<-ABCtable[3]
    
####end modification    
    A.Arm.Time<-time1
    B.Arm.Time<-time2
    C.Arm.Time<-time3
    
    ###change MNBF.ID to NBP.ID 
    
    Animal<-cbind(Trial.Number,
                  Arena,
                  NBP.ID,
                  Entry.Sequence,
                  Total.Arm.Entries,
                  Number.of.Triplets,
                  Number.of.Spontaneous.Alternations,
                  Alternation.Ratio,
                  Latency.to.Leave.Start.Arm,
                  A.Arm.Entry,B.Arm.Entry,C.Arm.Entry,
                  A.Arm.Time,B.Arm.Time,C.Arm.Time,
                  Date,
                  Time)
    Ymaze<-rbind(Ymaze,Animal)
    
    cat(Date, "   ", Trial.Number,"   ", Arena,"   ", "is now finished", "\n")
  }
}

write.csv(Ymaze,file=output.file,row.names = FALSE)

proc.time() - ptm

