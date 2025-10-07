import pandas as pd
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from typing import Union, List, Optional, Tuple, Any, Iterable




# -- EMAIL -- #
def send_mail(mail_sender: str = 'GruppoNPIDemandPlanning@luxottica.com',
            mail_recipients: tuple |list | None | Iterable[str] = None,
            cc_recipients: tuple| list| None = None,
            ccn_recipients: tuple | list | None = None,
            subject: str = "subject - python email",
            text: str = "text - python email",
            attachments: tuple | list | None = None,
            smtp_server: str = 'smtp.luxottica.com',
            smtp_port: int = 25) -> None:
    """Send email containing specified text, subject, attachment, etc.

    Parameters
    ---------
    mail_sender: 
        Sender (From:) of the mail
    mail_recipients: 
        Recipients (To:) of the mail
    cc_recipients: 
        Recipients in CC (CC:) of the mail
    ccn_recipients: 
        Recipients in CCN (CCN:) of the mail
    subject: 
        Subject of the mail
    text: 
        Text of the mail
    attachments: 
        Complete Paths of the file(s) to be attached
    smtp_server: 
        SMTP server to be used
    smtp_port: 
        SMTP port of the server to be used
    """
    msg = MIMEMultipart()
    msg['From'] = mail_sender
    msg['To'] = ', '.join(mail_recipients)
    mail_recipients = msg['To'].split(', ')
    if cc_recipients is not None:
        msg['Cc'] = ', '.join(cc_recipients)
        mail_recipients += msg['Cc'].split(', ')
    if ccn_recipients is not None:
        msg['Ccn'] = ', '.join(ccn_recipients)
        mail_recipients += msg['Ccn'].split(', ')
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    for f in attachments or []:
        if isinstance(f, Path):
            f = f.as_posix()
        with open(f, "rb") as file:
            ext = f.split(".")[-1:]
            attachedfile = MIMEApplication(file.read(), _subtype=ext)
            attachedfile.add_header("content-disposition", "attachment", filename=os.path.basename(f))
        msg.attach(attachedfile)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.sendmail(mail_sender, mail_recipients, msg.as_string())
        server.close()


# -- DATAFRAME -- #
def import_masterdata(
    file_path: Union[str, Path] = Path(__file__).parents[2] / "data" / "master_data" / "master_data.xlsx",
    sheet_name: Optional[str] = 0,
    index_col: Optional[Union[int, str]] = None,
    usecols: Optional[Union[List[int], List[str]]] = None,
    dtype: Optional[dict] = None,
    **kwargs: Any
) -> pd.DataFrame:
    """Import master data from 'data/master_data/master_data.xlsx'."""
    return pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        index_col=index_col,
        usecols=usecols,
        dtype=dtype,
        **kwargs
)