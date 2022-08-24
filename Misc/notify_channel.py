import json
import sys
import random
import requests

def notify_slack(message, title, color):
    url = ""
    message = (message)
    title = (title)
    slack_data = {
            "type": "home",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": title
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    },
                    "block_id": "text1"
                }
            ]
        }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

def format_metric(list):

    final_message = "```\n"
    for i in list:
        row = str(i).replace("(", "")
        row = row.replace(")", "")
        row = row.replace("'", "")
        row = row.replace(", ", "|")

        final_message += row + "\n"
    if not list:
        final_message += 'No difference found.'
    final_message += "```"

    return final_message

if __name__ == '__main__':
    notify_slack("message", "title", "#00FF00")