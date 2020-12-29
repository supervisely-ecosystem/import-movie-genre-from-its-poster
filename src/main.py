import os
import requests
import csv

import supervisely_lib as sly

my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])

INPUT_FILE = os.environ['modal.state.slyFile']
PROJECT_NAME = "Movie genre from its poster"
DATASET_NAME = "ds0"


def parse_genres(val):
    return list(set([v for v in val.split('|') if v]))


def download_file(url, local_path, logger, cur_image_index, total_images_count):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_path

@my_app.callback("transform")
@sly.timeit
def transform(api: sly.Api, task_id, context, state, app_logger):
    storage_dir = my_app.data_dir

    project = api.project.create(WORKSPACE_ID, PROJECT_NAME, change_name_if_conflict=True)
    dataset = api.dataset.create(project.id, DATASET_NAME, change_name_if_conflict=True)

    local_file = os.path.join(storage_dir, sly.fs.get_file_name_with_ext(INPUT_FILE))
    api.file.download(TEAM_ID, INPUT_FILE, local_file)


    tag_names = set()
    movies_info = []
    with open(local_file, encoding="ISO-8859-1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movies_info.append(row)
            tag_names.update(parse_genres(row["Genre"]))


    tags_arr = [sly.TagMeta(name=tag_name, value_type=sly.TagValueType.NONE) for tag_name in tag_names]
    project_meta = sly.ProjectMeta(tag_metas=sly.TagMetaCollection(items=tags_arr))
    api.project.update_meta(project.id, project_meta.to_json())
    movies_info_len = len(movies_info)
    movies_info_len_digits = len(str(movies_info_len))
    batch_size = 50

    progress = sly.Progress('Uploading images', movies_info_len, app_logger)
    for batch_idx, batch in enumerate(sly._utils.batched(movies_info, batch_size)):
        image_paths = []
        image_names = []
        image_metas = []
        for idx, csv_row in enumerate(batch):
            image_url = csv_row["Poster"]
            cur_img_ext = os.path.splitext(image_url)[1]
            cur_img_idx = str(batch_idx * batch_size + idx + 1).rjust(movies_info_len_digits, '0')
            image_name = f"{cur_img_idx}{cur_img_ext}"
            local_path = os.path.join(storage_dir, image_name)

            try:
                download_file(image_url, local_path, app_logger, batch_idx*batch_size+idx, movies_info_len)
            except:
                app_logger.warn(f"Couldn't download image:(row={batch_idx*batch_size+idx}, url={image_url}")
                continue

            image_paths.append(local_path)
            image_names.append(image_name)
            image_metas.append({
                "Title": csv_row["Title"],
                "imdbId": csv_row["imdbId"],
                "IMDB Score": csv_row["IMDB Score"],
                "Imdb Link": csv_row["Imdb Link"]
            })

        images = api.image.upload_paths(dataset.id, image_names, image_paths, metas=image_metas)
        cur_anns = []
        for image, csv_row in zip(images, batch):
            tags_arr = []
            image_tags = parse_genres(csv_row["Genre"])
            if len(image_tags) == 0:
                continue

            for image_tag in image_tags:
                tag_meta = project_meta.get_tag_meta(image_tag)
                tags_arr.append(sly.Tag(tag_meta))

            tags_arr = sly.TagCollection(items=tags_arr)
            ann = sly.Annotation(img_size=(image.height, image.width), img_tags=tags_arr)
            cur_anns.append((image.id, ann))

        if len(cur_anns) > 0:
            img_ids = [img_id for img_id, ann in cur_anns]
            anns = [ann for img_id, ann in cur_anns]
            api.annotation.upload_anns(img_ids, anns)

        progress.iters_done_report(len(batch))
    api.task.set_output_project(task_id, project.id, project.name)
    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "context.teamId": TEAM_ID,
        "context.workspaceId": WORKSPACE_ID,
        "CONFIG_DIR": os.environ.get("CONFIG_DIR", "ENV not found")
    })

    api = sly.Api.from_env()

    # Run application service
    my_app.run(initial_events=[{"command": "transform"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)
