import logging
import pandas as pd
import yaml
from datetime import datetime
from typing import Dict, Any
import imaplib
import email
import re

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class VeeTrackAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.objects = self.config["veetrack_news"]["objects"]
        self.secrets = secrets
        self.wrangler = DataWrangler(secrets)

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is not active or missing in config.")
        return obj

    def _extract_email_body_sectionwise(self, obj_cfg: Dict[str, Any], msg: email.message.Message) -> str:
        body = ""
        for part in msg.walk():
            if part.get_content_type() in ["text/plain", "text/html"]:
                body = part.get_payload(decode=True).decode(errors="ignore")
                break
        texts = re.sub(r'\r|\t|&nbsp;?', '', body)
        text = re.sub(r'\n{2,}', '\n', texts).strip()
        section_pattern = r'(?:^|\n)(' + "|".join(map(re.escape, obj_cfg["section_pattern"])) + r')\n'
        sections = re.split(section_pattern, text)
        return sections

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"Starting extraction for {object_name} under get_dataframe()")
        obj_cfg = self._get_obj_cfg(object_name)

        mail = imaplib.IMAP4_SSL(self.secrets["IMAP_SERVER"])
        mail.login(self.secrets["EMAIL_USER"], self.secrets["EMAIL_PASSCODE"])
        mail.select("inbox")
        today_str = datetime.now().strftime("%d-%b-%Y")     #DD-MMM-YYYY (e.g., 30-Oct-2025)
        # today_str = '12-May-2026'
        logging.info(f"Today's date in string format :{today_str}")
        search_query = f'(FROM "{obj_cfg["emailer"]}" SENTSINCE {today_str})'
        status, data = mail.search(None, search_query)
        if status != "OK" or not data[0]:
            logging.warning(f"⚠️No emails found from @veetrack Media for {today_str}.")
            mail.logout()
            return pd.DataFrame()

        email_ids = data[0].split()
        logging.info(f"📧 Found {len(email_ids)} email(s) from VeeTrack for {today_str}")
        articles = []
        for email_id in email_ids:
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            sections = self._extract_email_body_sectionwise(obj_cfg, msg)
            for i in range(1, len(sections), 2):
                section_name = sections[i].strip()
                block = sections[i + 1].strip()

                matches = re.findall(
                    r'(\d{2}/\d{2}/\d{4})\s*\n([^\n]+)\n([^\n]+)\n([^\n<>]+<https?://[^\n>]+>)\n([^\n]+)',
                    block
                )
                for article_date, headline, article, publication_link, edition in matches:
                    articles.append({
                        "section": section_name,
                        "article_date": article_date.strip(),
                        "headline": headline.strip(),
                        "article_content": article.strip(),
                        "publication": publication_link.strip(),
                        "edition": edition.strip()
                    })
        vee_df = pd.DataFrame(articles)
        if vee_df.empty:
            logging.warning("⚠️ No articles parsed from email.")
            return vee_df

        vee_df[['publication', 'link']] = vee_df['publication'].str.extract(r'([^<]+)<(https?://[^>]+)>')
        vee_df['publication'] = vee_df['publication'].str.strip()
        vee_df['link'] = vee_df['link'].str.strip()
        print(f"section-wise Bifurcations: --> {vee_df['section'].value_counts().to_dict()}")
        logging.info(f"✅Extracted {len(vee_df)} articles across all sections.---> Loading to save_batch..")

        self.wrangler.save_batch(vee_df, "veetrack", "veetrack_news", primary_key=obj_cfg["primary_key"])
        logging.info(f"Completed save_batch() of VeeTrack for {today_str}.")
        return pd.DataFrame()