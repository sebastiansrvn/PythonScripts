import subprocess

def open_apps(cmd):
    p = subprocess.Popen(cmd,shell=True,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr=p.communicate() 

def main():
    cmd_list = ["Spotify", "Slack", "Safari", "Visual\ Studio\ Code", "Spectacle", "Google\ Chrome", "Asana"]

    for cmd in cmd_list:
        temp_string = "open /Applications/" + cmd + ".app"
        open_apps(temp_string)

if __name__ == "__main__":
    main()