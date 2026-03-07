import { spawn } from 'child_process';
import { config } from 'dotenv';
import fs from 'fs';
import { pipeline } from 'stream/promises';
import { GetObject, PutObject } from "@spacelabs-cloud/cosmic/bytelake";
import { SendMail } from "@spacelabs-cloud/cosmic/raven";

config();

async function downloadVideo(urn) {
  const readStream = await GetObject(urn);

  await pipeline(
    readStream.data,
    fs.createWriteStream("video.mp4")
  );
}

function spawnFFmpeg(args, label) {
    return new Promise((resolve, reject) => {
        const proc = spawn('ffmpeg', args);

        proc.stderr.on('data', d => console.error(d.toString()));
        proc.stdout.on('data', d => console.log(d.toString()));

        proc.on('close', code => {
            if (code === 0) resolve();
            else reject(new Error(`${label} exited with code ${code}`));
        });

        proc.on('error', reject);
    });
}

async function uploadVideo(lakeName, videoName) {
    const res = await PutObject(lakeName, fs.createReadStream(videoName), videoName);

    return res.fileName;
}

async function main() {
    await downloadVideo(process.env.VIDEO_URN);

    const partName = 'output.mp4';

    await spawnFFmpeg([
        '-ss', process.env.START_TIME,
        '-to', process.env.END_TIME,
        '-i', 'video.mp4',
        '-c', 'copy',
        partName
    ], 'cutter');

    const fileName = await uploadVideo(process.env.DESTINATION_LAKE, partName);

    const destinations = [];
    destinations.push(process.env.EMAIL_TO);

    await SendMail({
        outbockId: process.env.OUTBOX_ID,
        from: "notifications",
        fromName: "EpicTV Notifications",
        to: destinations,
        subject: 'Your video is ready',
        text: `Your video has been processed and is available at: http://assets.epictv.pt/${fileName}`,
        html: `<p>Your video has been processed and is available: <a href="http://assets.epictv.pt/${fileName}" target="_blank">Click to watch</a></p>`
    });

    console.log('Video generated successfully');
}

main().catch(err => {
    console.error(err);
    process.exitCode = 1;
});