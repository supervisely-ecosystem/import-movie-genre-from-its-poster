import os
import requests
import csv

import supervisely_lib as sly

my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])

INPUT_FILE = os.environ['modal.state.slyFile']
PROJECT_NAME = "Movies Metadata from its Posters"
DATASET_NAME = "ds0"

USE_TAG_VALUE = True


def upload_valid_links(api, dataset_id, names, urls):
    try:
        api.image.upload_links(dataset_id, names, urls)
    except requests.exceptions.HTTPError as e:
        data = e.response.json()

        invalid_names_set = set([img["name"] for img in data['details']])
        invalid_urls_set = set([img["link"] for img in data['details']])

        valid_names = [v for v in names if v not in invalid_names_set]
        valid_urls = [v for v in urls if v not in invalid_urls_set]

        if len(valid_names) > 0:
          return api.image.upload_links(dataset_id, valid_names, valid_urls)

        return []


@my_app.callback("transform")
@sly.timeit
def transform(api: sly.Api, task_id, context, state, app_logger):
    storage_dir = my_app.data_dir

    project = api.project.create(WORKSPACE_ID, PROJECT_NAME, change_name_if_conflict=True)
    dataset = api.dataset.create(project.id, DATASET_NAME, change_name_if_conflict=True)

    local_file = os.path.join(storage_dir, sly.fs.get_file_name_with_ext(INPUT_FILE))
    api.file.download(TEAM_ID, INPUT_FILE, local_file)

    if USE_TAG_VALUE:
        val_type = sly.TagValueType.ANY_STRING
    else:
        val_type = sly.TagValueType.NONE

    tag_names = ["Genre", "Title", "imdbId", "IMDB Score"]
    tags_arr = [sly.TagMeta(name=tag_name, value_type=val_type) for tag_name in tag_names]

    project_meta = sly.ProjectMeta(tag_metas=sly.TagMetaCollection(items=tags_arr))
    api.project.update_meta(project.id, project_meta.to_json())

    movies_info = {}
    with open(local_file, encoding="ISO-8859-1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movies_info[row["Poster"]] = row

    progress = sly.Progress("Processing {} dataset".format(DATASET_NAME), len(movies_info), sly.logger)
    for batch in sly._utils.batched(list(movies_info.values())):
        image_urls = []
        image_names = []
        for idx, csv_row in enumerate(batch):
            image_url = csv_row["Poster"]
            image_urls.append(image_url)
            image_name = sly.fs.get_file_name_with_ext(image_url)
            image_names.append(image_name)

        app_logger.info("Processing [{}/{}]: {!r}".format(idx, len(batch), image_urls))
        images = upload_valid_links(api, dataset.id, image_names, image_urls)

        cur_anns = []
        for image in images:
            csv_row = movies_info[image.link]
            tags_arr = []
            for tag_name in tag_names:
                if tag_name == "Genre":
                    image_tags = csv_row[tag_name].split('|')
                else:
                    image_tags = [csv_row[tag_name]]

                tag_meta = project_meta.get_tag_meta(tag_name)
                if USE_TAG_VALUE:
                   for tag in image_tags:
                       tags_arr.append(sly.Tag(tag_meta, value=tag))
                else:
                    tags_arr.append(sly.Tag(tag_meta))

            tags_arr = sly.TagCollection(items=tags_arr)
            ann = sly.Annotation(img_size=(image.height, image.width), img_tags=tags_arr)
            cur_anns.append(ann)

        img_ids = [x.id for x in images]
        api.annotation.upload_anns(img_ids, cur_anns)

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
