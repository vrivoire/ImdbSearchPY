# cd C:\Users\rivoi\Documents\NetBeansProjects\PycharmProjects\ImdbSearch
# pyinstaller --onefile main.py --icon=C:\Users\rivoi\Documents\NetBeansProjects\PycharmProjects\ImdbSearch\IMDb.ico --nowindowed --noconsole --paths C:\Users\rivoi\Documents\NetBeansProjects\PycharmProjects\ImdbSearch\venv\Lib\site-packages
from imdb import Cinemagoer, IMDbError
import imdb.Person as Person
import imdb.Company as Company
import imdb
import json
import os
from datetime import datetime
from concurrent.futures.thread import ThreadPoolExecutor
import sys
import traceback
import urllib.request

SUPPORTED_EXTENSIONS = None
IGNORED_FOLDERS = None
search_path = None
props = {}


def load_data(title):
    prop = {}
    movie = None
    try:
        ia = Cinemagoer()
        movies = ia.search_movie(title)
        movie = movies[0]
        movie = ia.get_movie(movie.movieID, info=['main', 'plot', 'awards'])
    except IMDbError as ex:
        print(f'2 ERROR {ex}: {title}')
        print(traceback.format_exc())
        raise ex
    finally:
        if movie is None:
            prop = {f'1 ERROR': f'1 - ************** IMDbError ************** Not found: {title}'}
            print(prop)
        else:
            for key in movie.infoset2keys:
                values = movie.infoset2keys[key]
                for value in values:
                    if type(movie.get(value)) is list and len(movie.get(value)) > 0 and (isinstance(movie.get(value)[0], Person.Person) or isinstance(movie.get(value)[0], Company.Company)):
                        pass
                    else:
                        prop[f'{key}.{value}'] = movie.get(value)
            try:
                prop['main.directors'] = []
                for val in movie['director']:
                    if len(val) > 0:
                        prop['main.directors'].append(val["name"])
            except KeyError:
                pass
            try:
                prop['main.writers'] = []
                for val in movie['writer']:
                    if len(val) > 0:
                        prop['main.writers'].append(val["name"])
            except KeyError:
                pass

        return prop


def save_json(prop):
    print(f'Writing file {OUTPUT_JSON_FILE}')
    with open(OUTPUT_JSON_FILE, "w", encoding="utf-8") as outfile:
        outfile.write(json.dumps(prop, indent=4, sort_keys=True))


def spawn(thread_index, titles):
    try:
        print(f'\tStarting thread_id: {thread_index}, Titles: {titles}')
        size = len(titles)
        i = 0
        global props

        for title in titles:
            # time.sleep(1)
            i += 1
            prop = {}
            try:
                title = title.replace('.', ' ')
            except Exception:
                pass  # No dot and extension
            finally:
                try:
                    prop = load_data(title)
                except Exception:
                    try:
                        prop = load_data(title)
                    except Exception as ex:
                        prop = {f'2 ERROR': f'2 - ************** IMDbError ************** thread_id: {thread_index}, {ex}: {title}'}
                        print(f'\t{prop}')
                        print(traceback.format_exc())
                finally:
                    print(f'\t\tthread_id: {thread_index}, {i}/{size}, found: {title}')
                    props.update({title: prop})
        print(f'\tEnding {thread_index}\r')
    except Exception as ex:
        prop = {f'3 ERROR': f'3 - ************** IMDbError ************** thread_id: {thread_index}, {ex}: {titles}'}
        print(f'\t{prop}')
        print(traceback.format_exc())


def args_search(files):
    print(f'Searching args {files}')
    file_count = len(files)
    thread_nb = THREAD_NB
    files_per_thread = int(len(files) / thread_nb)
    remain_files = file_count - files_per_thread * thread_nb
    print(f'file_count: {file_count}, thread_nb: {thread_nb}, files_per_thread: {files_per_thread}, remain_files: {remain_files}, toto: {file_count / thread_nb}')

    with ThreadPoolExecutor(max_workers=thread_nb + 1) as executor:
        i = 0
        for thread_id in range(1, thread_nb + 1):
            k = thread_id * files_per_thread
            print(f'thread_id: {thread_id}, {range(i, k)}, size: {len(files[i:k])}')
            executor.submit(spawn, thread_id, files[i:k])
            i = k
        print(f'thread_id: {thread_nb + 1}, {range(file_count - remain_files, file_count)}, size: {len(files[file_count - remain_files:file_count])}')
        executor.submit(spawn, thread_nb + 1, files[file_count - remain_files:file_count])
    print('All tasks has been finished')
    save_json(props)

    print(f"Threads: {thread_nb}, Time elapsed: {datetime.fromtimestamp(datetime.timestamp(datetime.now()) - start).strftime('%M:%S.%f')} for {len(files)} titles, {datetime.fromtimestamp((datetime.timestamp(datetime.now()) - start) / len(files)).strftime('%M:%S.%f')} per title.")


def path_search(path):
    print(f'Searching into {path}')
    if os.path.isfile(OUTPUT_JSON_FILE):
        os.remove(OUTPUT_JSON_FILE)

    files = os.listdir(path)
    for i, file in enumerate(files):
        if IGNORED_FOLDERS.__contains__(file) or not file.endswith(SUPPORTED_EXTENSIONS) and not os.path.isdir(path + file) and file.endswith('.html'):
            files.remove(file)
        files[i] = file[0:len(file) - 4]
    args_search(files)


if __name__ == "__main__":
    start = datetime.timestamp(datetime.now())

    master_version = ''
    for line in urllib.request.urlopen('https://raw.githubusercontent.com/cinemagoer/cinemagoer/master/imdb/version.py'):
        master_version = line.decode('utf-8')
    master_version = master_version.removeprefix("__version__ = '").replace("'", "").strip()
    print(f'imdb={imdb}')
    print(f'imdb.VERSION={imdb.VERSION}, master_version={master_version}')
    if imdb.VERSION != master_version:
        print("********************************************************************************************************************************")
        print("********************************************************************************************************************************")
        print("********************************************************************************************************************************")
        print(f'                                 UPDATE Cinemagoer to version {master_version}')
        print("********************************************************************************************************************************")
        print("********************************************************************************************************************************")
        print("********************************************************************************************************************************")

    print('********************* Test to see if "awards" are still broken *********************')
    cinemagoer = Cinemagoer()

    test = cinemagoer.get_movie(34583, info=['main', 'awards'])
    print(f"movie['title']: {test['title']}, movie.get('awards'): {test.get('awards')}, ia.get_movie_awards(movie.movieID): {cinemagoer.get_movie_awards(test.movieID)}")
    print('************************************************************************************')

    CONFIG = json.load(open("{HOMEDRIVE}{HOMEPATH}/Documents/NetBeansProjects/ImdbSearch/bin/config.json".format(**os.environ)))
    for line in CONFIG:
        CONFIG[line] = str(CONFIG[line]).replace('${', '{').format(**os.environ)
    SUPPORTED_EXTENSIONS = tuple(CONFIG["SUPPORTED_EXTENSIONS"])
    IGNORED_FOLDERS = tuple(CONFIG["IGNORED_FOLDERS"])
    THREAD_NB = int(CONFIG["THREAD_NB"])
    OUTPUT_JSON_FILE = CONFIG["OUTPUT_JSON_FILE"].replace('${', '{').format(**os.environ)

    print(f'sys.argv={sys.argv}')
    if len(sys.argv[1:]) > 0:
        print("Custom args.")
        args_search(sys.argv[1:])
    else:
        print("Default path.")
        # path_search(str(Path.home()) + os.sep + "Videos" + os.sep)
        # path_search(str(Path.home()) + os.sep + "Videos" + os.sep + "W" + os.sep)
        # path_search("D:/Films/W2/")
        # path_search("C:/Users/rivoi/Videos/W/Underworld")
        path_search("C:/Users/rivoi/Videos/W/Underworld")
