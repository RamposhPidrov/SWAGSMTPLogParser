import re
#import file

def NicePrint(regmatch):
    print(str.format("Time {0} IP {1:<16} Type {2} Content {3}", regmatch.group(1), regmatch.group(3), regmatch.group(5), regmatch.group(10)))
#f = file.rea open("SMTP.log", "r+")


if __name__ == "__main__":
    with open('SMTP.log') as f:
            for line in f:
                reg = re.search(r"(\d{2}/\d{2}/\d{2} \d\d:\d\d:\d\d)(?!\s\d)(.+)(?<=\d\s)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\s{2})?(?(4)|\s([A-Z]{4})\s([^\s]{4,}))(\s)?(?(7) ((.)+(?!\d)) | *)\s(.{1,})(?<!\d)(\d{1,})\s(\d{1,})\s((.)+)" , line)
                try:    
                    
                    NicePrint(reg)
                except:
                    pass
#str = "10/19/18 00:00:25	SMTP-IN	FE7A5EF7F5334F7ABB9DD341121BCE8F.MAI	816	191.96.249.14			220 mail.cirkul-m.ru ESMTP MailEnable Service, Version: 9.76-- ready at 10/19/18 00:00:25	0	0 10/19/18 00:00:35	SMTP-IN	25E7EB033CE24B45B7CD8E3EEEE8726F.MAI	908	191.96.249.24	AUTH	MTIzNDU2Nzg5	535 Invalid Username or Password	34	14	xmpp@mail.cirkul-m.ru	"
#d = re.finditer(r"(\d{2}/\d{2}/\d{2} \d\d:\d\d:\d\d)(?!\s\d)(.+)(?<=\d\s)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(\s{2})?(?(4)|\s([A-Z]{4})\s([^\s]{4,}))(\s)?(?(7) ((.)+(?!\d)) | *)\s(.{1,})(?<!\d)(\d{1,})\s(\d{1,})\s((.)+)", str ) 

#for match in dd:
#    print(match)