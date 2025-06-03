import requests
import time


def downloadtest(url):
    starttime = time.time()
    response = requests.get(url, stream=True)
    totaldownloaded = 0

    for chunk in response.iter_content(chunk_size=1024):
        if chunk:
            totaldownloaded += len(chunk)

    endtime = time.time()
    elapsedtime = endtime - starttime
    downloadspeed = (
        totaldownloaded / (1024 * 1024) / elapsedtime
    )  # Convert to Mbps

    return downloadspeed


# Example URL for a large file
url = "http://speedtest.tele2.net/100MB.zip"
downloadspeed = downloadtest(url)
print(f"Download Speed: {downloadspeed:.2f} Mbps")
