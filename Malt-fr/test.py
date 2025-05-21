import requests

url = "https://www.malt.fr/profile/67cf172cf8bcd42ab212a30d"

payload = {}
headers = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
  'priority': 'u=0, i',
  'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'none',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
  'Cookie': 'malt-visitorId=890b144a-3598-4bd4-8229-81bc983507e8; OptanonAlertBoxClosed=2025-04-17T12:12:06.904Z; _gcl_au=1.1.139234146.1744891961; _fbp=fb.1.1744893115162.530334627927976086; i18n=fr-FR; remember-me=a2dZUVJ2SXlES2lCZGJvUnZTanZTUSUzRCUzRDp1SFE2RllzZFB6OXlyQzZuUjdhbVp3JTNEJTNE; XSRF-TOKEN=812e6b4d-46ef-4136-89f6-f0bb8aac62db; hopsi=67e1607b991b4d70bad837d1; SESSION=MDM5NjJmZDYtNmVmYi00ZmI2LWJiODctZmIzMTIyZGRkZDll; JSESSIONID=90A5EEA3238B6B62A8CAA4F82891D317; __cfruid=de9e12aafd4c0dc6e0be65cd6370e4dbc9d0a5d6-1747665367; __cf_bm=upEuVX_khLA.K.3y21GffBfTBgJBZoyAF_xnrWxxjTA-1747665367-1.0.1.1-5jN.oB92fPEg._rmBQxqp5fsHGDN7bI0Dezvuc6TxRgUqz5xafxpj0CpUYicmUHg9wdP2Jx5KePBzIxRnC4wJkHLnYxJFbi.yREKHjmFN8ny8dKZHMlDAAAHrw1pC0bX; cf_clearance=bkJOoh2Uv11fhSTOmyL2ct3Y0AOjiBbYYCpm1U9PIMU-1747665738-1.2.1.1-YETeTMSvOtxRWBGiZ8l1hGHvhHdOBJedXNp8m4wB.uU68vf026EwDr9jAdmRJ7Cyz0qabogoe5zGEg3FRwTYP5G0YwF1S09jmkG2N4GVjPWC9etIzPVbXzXocgY.OJPjtWfOe5eaPDvnW8gvGDPustqmjZIx278.qJ8KhbNo6qiMGrR3PHVDDpIWbbZ2EiKZvwacZR_sXrVfKflbxLwW7kpPTGc_eX4nft7zJnF70t.z4hKRwHX7Ih2Jb_QxBVwkuyS7cZFqkd1mq9H62_VXBfcL5N3EB0Lg5QoBL4UnPxAasV8VUdmMor1JtSownk1hBI6WWjy2P12M5znczHZv9Ta4qP7g1IMHYUKCeeHnk_w; OptanonConsent=isGpcEnabled=0&datestamp=Mon+May+19+2025+16%3A45%3A31+GMT%2B0200+(Central+European+Summer+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=c99c6217-5697-4401-8fbd-61a443497739&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0005%3A1&geolocation=%3B&AwaitingReconsent=false; __cf_bm=Hfpbm7VrgxXhpJGVv8g2H5lIBE7jVRNnC0y1JFBUhfw-1747674566-1.0.1.1-6mfAEtijz1c49D2YN5wC8ck8dCQby5RB9fGiT_mXsAYkitd1ym7lkGAvFeXorm49Cl7I1NWDeLjCHErIEAD6opHpVoZQaXXRbSPXxS3GGDHNXf7tYh328YRwaYfYZovo; __cfruid=406b0606a67dd6ca1c84dc557ae87d655ebbec4d-1747665807; JSESSIONID=8A61CB1175799BB6D5A465893E02355D; SESSION=NjMxNWFmYzgtZGM5Ni00M2Y2LWFiNzAtYjE5ZmUzZTEzZWE2; malt-visitorId=890b144a-3598-4bd4-8229-81bc983507e8'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
