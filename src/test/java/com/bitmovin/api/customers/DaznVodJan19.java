package com.bitmovin.api.customers;

import com.bitmovin.api.BitmovinApi;
import com.bitmovin.api.encoding.*;
import com.bitmovin.api.encoding.codecConfigurations.AACAudioConfig;
import com.bitmovin.api.encoding.codecConfigurations.CodecConfig;
import com.bitmovin.api.encoding.codecConfigurations.ColorConfig;
import com.bitmovin.api.encoding.codecConfigurations.H264VideoConfiguration;
import com.bitmovin.api.encoding.codecConfigurations.H265VideoConfiguration;
import com.bitmovin.api.encoding.codecConfigurations.enums.*;
import com.bitmovin.api.encoding.encodings.Encoding;
import com.bitmovin.api.encoding.encodings.EncodingMode;
import com.bitmovin.api.encoding.encodings.StartEncodingRequest;
import com.bitmovin.api.encoding.encodings.muxing.MP4Muxing;
import com.bitmovin.api.encoding.encodings.muxing.MuxingStream;
import com.bitmovin.api.encoding.encodings.muxing.broadcastTs.BroadcastProgramConfiguration;
import com.bitmovin.api.encoding.encodings.muxing.broadcastTs.BroadcastTsAudioInputStreamConfiguration;
import com.bitmovin.api.encoding.encodings.muxing.broadcastTs.BroadcastTsMuxing;
import com.bitmovin.api.encoding.encodings.muxing.broadcastTs.BroadcastTsMuxingConfiguration;
import com.bitmovin.api.encoding.encodings.muxing.broadcastTs.BroadcastTsTransportConfiguration;
import com.bitmovin.api.encoding.encodings.muxing.broadcastTs.BroadcastTsVideoInputStreamConfiguration;
import com.bitmovin.api.encoding.encodings.muxing.enums.RAIUnit;
import com.bitmovin.api.encoding.encodings.streams.Stream;
import com.bitmovin.api.encoding.enums.CloudRegion;
import com.bitmovin.api.encoding.enums.StreamSelectionMode;
import com.bitmovin.api.encoding.filters.DeinterlaceFilter;
//import com.bitmovin.api.encoding.filters.UnsharpFilter;
import com.bitmovin.api.encoding.inputs.GcsInput;
import com.bitmovin.api.encoding.inputs.Input;
import com.bitmovin.api.encoding.outputs.Output;
import com.bitmovin.api.encoding.status.Task;
import com.bitmovin.api.enums.Status;
import com.bitmovin.api.exceptions.BitmovinApiException;
import com.bitmovin.api.http.RestException;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Jonathan Perry on 07/18/2018.
 * Adapted by Fabre Lambeau on 01-Feb-19
 */
public class DaznVodJan19
{
    // Solutions@bitmovin.com
    private static String API_KEY = "b6167490-9d06-4362-a21f-bfadbdbe804e";

    private static CloudRegion CLOUD_REGION = CloudRegion.GOOGLE_EUROPE_WEST_1;
    private static String GCS_INPUT_ID = "7170dab0-187c-4cd4-9c89-ae05521a11a8";
    private static String GCS_OUTPUT_ID = "fbbbe78e-27a5-4f6d-a9f8-56168069396a";

    private static String ENCODER_VERSION = "NIGHTLY";
    private static EncodingMode ENCODING_MODE = EncodingMode.STANDARD;
    private static String RELATIVE_INPUT_PATH = "analysis/perform/vod-jan-2019/";
    private static List<String> FILES = Arrays.asList(
            "MOT_F1_HL_181111_Brazil_Race_ja_1541969119243.mxf"
            //"SOC_EPL_HL_181111_MCI-MUN_MD12_de_1541984683866.mxf",
            //"SOC_SEA_HL_181111_SAS-LAZ_MD12_it_1541965846814.mxf",
            //"SOC_UCL_HL_1801106_AMA-BVB_MD4_en_1541553866489.mxf"
    );
    private static SimpleDateFormat FORMATTER = new SimpleDateFormat("YYYYMMdd'T'HHmmss");
    private static String OUTPUT_BASE_PATH = "perform/vod-may-2019";

    private static String COLLECTION = "cbr-vbv1sec";

    private static String DEINTERLACE_FILTER_FRAME = "0c72a2b3-8f47-4a9d-9af5-af51f96e4a88";
    private static String DEINTERLACE_FILTER_FIELD = "23d68d5f-62dd-413d-99a2-d5e1cd95e397";

    private static Float BUFSIZE_IN_SECONDS = 1f;
    private static Integer FRAGMENT_DURATION = 1920;

    private static Boolean POLL_STATUS = false;

    private static String JSON_FILE = "/Users/fabre.lambeau/Downloads/config.json";

    private static BitmovinApi bitmovinApi;

    // track selection is 0-index based
    private List<OutputConfig> performMP4Configs = Arrays.asList(
            new OutputConfig("H264", ProfileH264.BASELINE, 192, 108, 25f, 112000L, 1.92f,0, 1,64000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.MAIN, 480, 270, 25f, 288000L, 1.92f, 3, 1,64000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.MAIN, 640, 360, 25f, 480000L, 1.92f,3,1,64000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.MAIN, 960, 540, 25f, 840000L, 1.92f,3,1,64000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.MAIN, 960, 540, 25f, 1500000L, 1.92f,3,1,128000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.MAIN, 1280, 720, 25f, 2300000L, 1.92f,3,1,128000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.MAIN, 1280, 720, 25f, 3000000L, 1.92f,3,2,128000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.HIGH, 1280, 720, 50f, 4400000L, 1.92f,3,4,128000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.HIGH, 1280, 720, 50f, 6500000L, 1.92f,3,4,128000L, new int[] {0}, null, false),
            new OutputConfig("H264", ProfileH264.HIGH, 1280, 720, 50f, 8000000L, 1.92f,3,4,128000L, new int[] {0}, null, false)
        );

    private List<OutputConfig> performCRFConfigs = Arrays.asList(
            new OutputConfig("H265", ProfileH264.HIGH, 1280, 720, 25f, 50_000_000L, 1.92f,3, 4,128000L, new int[] {0}, null, false),
            new OutputConfig("H265", ProfileH264.HIGH, 1280, 720, 50f, 50_000_000L, 1.92f,3, 4,128000L, new int[] {0}, null, false)
    );

    @Before
    public void setUp() throws IOException
    {
        bitmovinApi = new BitmovinApi(API_KEY);
        bitmovinApi.setDebug(true);
    }


    @Test
    @Ignore
    public void testCreateCrf0Pass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("CRF0", EncodingMode.SINGLE_PASS, true, false, false, false, false, true, performCRFConfigs));

    }

    @Test
    public void testCreateSinglePass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("SINGLEPASS", EncodingMode.SINGLE_PASS, false, false, false, false, false, true, performMP4Configs));

    }

    @Test
    public void testCreateTwoPass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("TWOPASS", EncodingMode.TWO_PASS, false, false, false, false, false, true, performMP4Configs));

    }

    @Test
    public void testCreateTwoPassCBR() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("TWOPASS-CBR", EncodingMode.TWO_PASS, false, false, false, false, true, false, performMP4Configs));

    }


    @Test
    public void testCreateThreePass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("THREEPASS", EncodingMode.THREE_PASS, false, false, false, false, false, true, performMP4Configs));

    }

    @Test
    public void testCreateSharpenTwoPass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("SHARPEN-TWOPASS", EncodingMode.TWO_PASS, false, false, true, false, false, true, performMP4Configs));

    }

    @Test
    public void testCreateDenoiseTwoPass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("DENOISE-TWOPASS", EncodingMode.TWO_PASS, false, false, false, true, false, true, performMP4Configs));

    }

    @Test
    public void testCreateBitrateReductionTwoPass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("BITDROP-TWOPASS", EncodingMode.TWO_PASS, false, true, false, false, false, true, performMP4Configs));

    }

    @Test
    public void testCreateBitrateReductionThreePass() throws BitmovinApiException, RestException, UnirestException, IOException, URISyntaxException, InterruptedException
    {
        createEncodings(new EncodingConfiguration("BITDROP-THREEPASS", EncodingMode.THREE_PASS, false, true, false, false, false, true, performMP4Configs));

    }


    private void createEncodings(EncodingConfiguration config)
            throws URISyntaxException, BitmovinApiException, RestException, UnirestException, IOException, InterruptedException
    {
        for (int i = 0; i < FILES.size(); i++)
        {
            String pathToFile = RELATIVE_INPUT_PATH + FILES.get(i);
            createEncoding(config, pathToFile);

        }
    }

    private void createEncoding(EncodingConfiguration config, String pathToFile)
            throws URISyntaxException, BitmovinApiException, RestException, UnirestException, IOException, InterruptedException
    {
        Encoding encoding = new Encoding();
        encoding.setName("Perform - VoD VQ - " + config.jobMode + " - " + getBasefilename(pathToFile));
        encoding.setCloudRegion(CLOUD_REGION);
        encoding.setEncoderVersion(ENCODER_VERSION);
        encoding = bitmovinApi.encoding.create(encoding);

        GcsInput input = bitmovinApi.input.gcs.get(GCS_INPUT_ID);

        Output output = bitmovinApi.output.gcs.get(GCS_OUTPUT_ID);

        InputStream inputStreamVideo = new InputStream();
        inputStreamVideo.setInputPath(pathToFile);
        inputStreamVideo.setInputId(input.getId());
        inputStreamVideo.setSelectionMode(StreamSelectionMode.VIDEO_RELATIVE);
        inputStreamVideo.setPosition(0);

        // OUTPUT_BASE_PATH / BASENAME / JOBMODE / (BASENAME_P00.mp4)
        String outputPath = String.format("%s/%s/%s/%s", OUTPUT_BASE_PATH, getBasefilename(pathToFile), COLLECTION, config.jobMode);

        for (int i = 0; i < config.outputConfigs.size(); i++)
        {
            OutputConfig outputConfig = config.outputConfigs.get(i);
            List<Stream> streams = new ArrayList<>();

            List<Stream> audio_streams = null;
            if (config.withAudio) {
                audio_streams = createAudioStreams(encoding, outputConfig, input, pathToFile);
                streams.addAll(audio_streams);
            }

            Map<String, StreamFilter> filters = createFilters(outputConfig.frameRate);
            Stream video_stream = createVideoStream(encoding, outputConfig, inputStreamVideo, filters, config);
            streams.add(video_stream);

            Integer profileId = i + 1;
            this.createMP4Muxing(encoding, output, streams, outputPath, buildBasename(pathToFile, outputConfig, profileId));

            BroadcastTsMuxingConfiguration muxingConfig = null;
            if (config.useCbr) {
                if (config.withAudio) {
                    muxingConfig = this.createConfigurationForBroadcastTsMuxing(video_stream.getId(), audio_streams.get(0).getId(), 0D);
                } else {
                    muxingConfig = this.createConfigurationForBroadcastTsMuxing(video_stream.getId(), null, 0D);
                }
                this.createBroadcastTsMuxing(
                        encoding, output, streams, outputPath, buildBasename(pathToFile, outputConfig, profileId), muxingConfig);
            }
        }

        StartEncodingRequest encodingRequest = new StartEncodingRequest();
        encodingRequest.setEncodingMode(config.encodingMode);

        bitmovinApi.encoding.start(encoding, encodingRequest);

        if (POLL_STATUS) {
            Task status = bitmovinApi.encoding.getStatus(encoding);

            while (status.getStatus() != Status.FINISHED && status.getStatus() != Status.ERROR)
            {
                status = bitmovinApi.encoding.getStatus(encoding);
                Thread.sleep(10000);
            }

            System.out.println(String.format("Encoding finished with status %s", status.getStatus().toString()));
        }
    }

    private List<Stream> createAudioStreams(Encoding encoding, OutputConfig outputConfig, Input input, String pathToFile)
            throws URISyntaxException, BitmovinApiException, UnirestException, IOException, RestException
    {
        List<Stream> streams = new ArrayList<>();

        if (outputConfig.getAudioBitrate() != null)
        {
            AACAudioConfig aacConfiguration = new AACAudioConfig();
            aacConfiguration.setBitrate(outputConfig.getAudioBitrate());
            aacConfiguration.setNormalize(true);
            aacConfiguration.setChannelLayout(ChannelLayout.CL_STEREO);
            aacConfiguration = bitmovinApi.configuration.audioAAC.create(aacConfiguration);

            Set<InputStream> audioStreams = new HashSet<>();
            for (int trackPosition : outputConfig.getAudioTracks())
            {
                InputStream inputStreamAudioTrack = new InputStream();
                inputStreamAudioTrack.setInputPath(pathToFile);
                inputStreamAudioTrack.setInputId(input.getId());
                inputStreamAudioTrack.setSelectionMode(StreamSelectionMode.AUDIO_RELATIVE);
                inputStreamAudioTrack.setPosition(trackPosition);
                audioStreams.add(inputStreamAudioTrack);
            }

            Stream audioStream = new Stream();
            audioStream.setCodecConfigId(aacConfiguration.getId());
            audioStream.setInputStreams(audioStreams);
            audioStream = bitmovinApi.encoding.stream.addStream(encoding, audioStream);

            streams.add(audioStream);
        }

        return streams;
    }

    private Stream createVideoStream(Encoding encoding, OutputConfig outputConfig, InputStream inputStreamVideo,
                                     Map<String, StreamFilter> filters, EncodingConfiguration config)
            throws BitmovinApiException, UnirestException, IOException, URISyntaxException, RestException
    {
        Stream videoStream = null;

        if (outputConfig.getVideoBitrate() != null)
        {
            CodecConfig videoConfig = null;
            if (StringUtils.equals(outputConfig.videoCodec, "H264"))
            {
                videoConfig = createH264Codec(outputConfig, config.useCrf0, config.reduceBitrateByPass, config.encodingMode);
            } else {
                videoConfig = createH265Codec(outputConfig, config.useCrf0);
            }

            videoStream = new Stream();
            videoStream.setCodecConfigId(videoConfig.getId());
            videoStream.setInputStreams(Collections.singleton(inputStreamVideo));

            StreamFilterList streamFilters = new StreamFilterList();
            streamFilters.getFilters().add(filters.get("deinterlace"));

            if (outputConfig.slices == 1)
            {
                if (config.useUnsharpFilter)
                {
                    streamFilters.getFilters().add(filters.get("unsharp"));
                }

                if (config.useDenoiseFilter)
                {
                    streamFilters.getFilters().add(filters.get("denoise"));
                }
            }

            videoStream.setFilters(streamFilters.getFilters());

            videoStream = bitmovinApi.encoding.stream.addStream(encoding, videoStream);

            bitmovinApi.encoding.stream.addFiltersToStream(encoding, videoStream, streamFilters);
        }

        return videoStream;
    }

    private CodecConfig createH264Codec(OutputConfig outputConfig, Boolean useCrf, Boolean reduceBitrate, EncodingMode encodingMode)
            throws URISyntaxException, BitmovinApiException, UnirestException, IOException
    {
        H264VideoConfiguration videoConfig = new H264VideoConfiguration();

        if (outputConfig.frameRate != null) {
            videoConfig.setRate(outputConfig.frameRate);
        }

        if (useCrf)
        {
            videoConfig.setCrf(0f);
        }
        else
        {
            videoConfig.setHeight(outputConfig.getHeight());
            Long videoBitrate = outputConfig.getVideoBitrate();

            if (reduceBitrate)
            {
                if (encodingMode == EncodingMode.THREE_PASS)
                {
                    videoBitrate = videoBitrate - Math.round(videoBitrate * .15);
                } else if (encodingMode == EncodingMode.TWO_PASS)
                {
                    videoBitrate = videoBitrate - Math.round(videoBitrate * .1);
                }
            }

            videoConfig.setBitrate(videoBitrate);
            videoConfig.setMaxBitrate(videoBitrate);
            videoConfig.setMinBitrate(videoBitrate);
            // Buffer size based on number of seconds
            videoConfig.setBufsize((long)(videoBitrate * BUFSIZE_IN_SECONDS));

            // GOP of 48 for 25fps, and 96 for 50fps
            videoConfig.setMaxGop((int)Math.round(outputConfig.frameRate * outputConfig.gopSize));
            videoConfig.setMinGop((int)Math.round(outputConfig.frameRate * outputConfig.gopSize));

            videoConfig.setLevel(outputConfig.getLevel());
            videoConfig.setNalHrd(H264NalHrd.CBR);
            videoConfig.setBframes(outputConfig.getBFrames());
            videoConfig.setSlices(outputConfig.getSlices());
    //        videoConfig.setSceneCutThreshold(0);
            videoConfig.setRefFrames(4);
        }

        videoConfig.setProfile(outputConfig.getProfile());

        videoConfig = bitmovinApi.configuration.videoH264.create(videoConfig);

        return videoConfig;
    }

    private CodecConfig createH265Codec(OutputConfig outputConfig, Boolean useCrf)
            throws URISyntaxException, BitmovinApiException, UnirestException, IOException
    {
        H265VideoConfiguration videoConfig = new H265VideoConfiguration();

        if (outputConfig.frameRate != null) {
            videoConfig.setRate(outputConfig.frameRate);
        }

        if (useCrf)
        {
            videoConfig.setCrf(0f);
        }

        ColorConfig colorConfig = new ColorConfig();
        colorConfig.setColorSpace(ColorSpace.BT709);
        colorConfig.setColorPrimaries(ColorPrimaries.BT709);
        colorConfig.setColorTransfer(ColorTransfer.BT709);
        videoConfig.setColorConfig(colorConfig);

        videoConfig.setProfile(ProfileH265.main10);
        videoConfig.setPixelFormat(PixelFormat.YUV422P);

        videoConfig = bitmovinApi.configuration.videoH265.create(videoConfig);


        return videoConfig;
    }

    private Map<String, StreamFilter> createFilters(Float frameRate) throws URISyntaxException, BitmovinApiException, UnirestException, IOException, RestException
    {
        Map<String, StreamFilter> filters = new HashMap<>();

        DeinterlaceFilter deinterlaceFilter = new DeinterlaceFilter();
//        deinterlaceFilter.setParity(PictureFieldParity.AUTO);
//        if (frameRate == 50f) {
//            deinterlaceFilter.setMode(DeinterlaceMode.FIELD);
//        }
//        if (frameRate == 25f) {
//            deinterlaceFilter.setMode(DeinterlaceMode.FRAME);
//        }
//        deinterlaceFilter = bitmovinApi.filter.deinterlace.create(deinterlaceFilter);

        if (frameRate == 50f) {
            deinterlaceFilter = bitmovinApi.filter.deinterlace.get(DEINTERLACE_FILTER_FIELD);
        }
        if (frameRate == 25f) {
            deinterlaceFilter = bitmovinApi.filter.deinterlace.get(DEINTERLACE_FILTER_FRAME);
        }

        StreamFilter deinterlaceStreamFilter = new StreamFilter();
        deinterlaceStreamFilter.setFilter(deinterlaceFilter);
        deinterlaceStreamFilter.setId(deinterlaceFilter.getId());
        deinterlaceStreamFilter.setPosition(0);

        filters.put("deinterlace", deinterlaceStreamFilter);

//        UnsharpFilter unsharpFilter = new UnsharpFilter();
//        unsharpFilter.setLumaMatrixVerticalSize(5);
//        unsharpFilter.setLumaMatrixVerticalSize(5);
//        unsharpFilter.setLumaEffectStrength(0);
//        unsharpFilter.setChromaMatrixVerticalSize(5);
//        unsharpFilter.setChromaMatrixHorizontalSize(5);
//        unsharpFilter.setChromaEffectStrength(0);
////        unsharpFilter = bitmovinApi.filter.unsharp.get("5b18768c-4d2f-4ba6-9e31-0085293db696");
//
//        StreamFilter unsharpStreamFilter = new StreamFilter();
//        unsharpStreamFilter.setFilter(unsharpFilter);
//        unsharpStreamFilter.setId(unsharpFilter.getId());
//        unsharpStreamFilter.setPosition(1);
//
//        filters.put("unsharp", unsharpStreamFilter);

//        DenoiseHqdn3dFilter denoiseFilter = new DenoiseHqdn3dFilter();
//        denoiseFilter.setLumaSpatial(4.0);
//        denoiseFilter.setLumaTmp((6.0 * 4.0) / 4.0);
//        denoiseFilter.setChromaSpatial((3.0 * 4.0) / 4.0);
//        denoiseFilter.setChromaTmp((denoiseFilter.getLumaTmp() * denoiseFilter.getChromaSpatial()) / denoiseFilter.getLumaSpatial());
////        denoiseFilter = bitmovinApi.filter.denoiseHqdn3d.get("d04b9fbd-667a-480c-a8d0-bcba539898f0");
//
//        StreamFilter denoiseStreamFilter = new StreamFilter();
//        denoiseStreamFilter.setFilter(denoiseFilter);
//        denoiseStreamFilter.setId(denoiseFilter.getId());
//        denoiseStreamFilter.setPosition(2);

//        filters.put("denoise", denoiseStreamFilter);

        return filters;
    }

    private void createMP4Muxing(Encoding encoding, Output output, List<Stream> streams, String outputBasePath, String filename) throws BitmovinApiException, IOException, RestException, URISyntaxException, UnirestException
    {
        EncodingOutput encodingOutput = new EncodingOutput();
        encodingOutput.setOutputId(output.getId());
        encodingOutput.setOutputPath(outputBasePath);
        encodingOutput.setAcl(new ArrayList<AclEntry>()
        {{
            this.add(new AclEntry(AclPermission.PUBLIC_READ));
        }});

        MP4Muxing mp4Muxing = new MP4Muxing();
//        mp4Muxing.setFragmentedMP4MuxingManifestType(FragmentedMP4MuxingManifestType.DASH_ON_DEMAND);
        mp4Muxing.setFragmentDuration(FRAGMENT_DURATION);
        mp4Muxing.setFilename(filename + ".mp4");
        mp4Muxing.setOutputs(Collections.singletonList(encodingOutput));
        List<MuxingStream> muxingStreams = new ArrayList<>();

        for (Stream stream: streams)
        {
            MuxingStream muxingStream = new MuxingStream();
            muxingStream.setStreamId(stream.getId());
            muxingStreams.add(muxingStream);
        }

        mp4Muxing.setStreams(muxingStreams);
        bitmovinApi.encoding.muxing.addMp4MuxingToEncoding(encoding, mp4Muxing);
    }

    public BroadcastTsMuxing createBroadcastTsMuxing(Encoding encoding, Output output, List<Stream> streams, String outputBasePath, String filename, BroadcastTsMuxingConfiguration configuration) throws BitmovinApiException, IOException, RestException, URISyntaxException, UnirestException
    {
        EncodingOutput encodingOutput = new EncodingOutput();
        encodingOutput.setOutputId(output.getId());
        encodingOutput.setOutputPath(outputBasePath);
        encodingOutput.setAcl(new ArrayList<AclEntry>()
        {{
            this.add(new AclEntry(AclPermission.PUBLIC_READ));
        }});

        BroadcastTsMuxing broadcastTsMuxing = new BroadcastTsMuxing();
        broadcastTsMuxing.setFilename(filename + ".ts");
        broadcastTsMuxing.setSegmentLength((double)FRAGMENT_DURATION / 1000);
        broadcastTsMuxing.setOutputs(Collections.singletonList(encodingOutput));

        for (Stream stream : streams)
        {
            MuxingStream muxingStream = new MuxingStream();
            muxingStream.setStreamId(stream.getId());
            broadcastTsMuxing.addStream(muxingStream);
        }

        broadcastTsMuxing.setConfiguration(configuration);
        return bitmovinApi.encoding.muxing.addBroadcastTsMuxingToEncoding(encoding, broadcastTsMuxing);
    }

    private BroadcastTsMuxingConfiguration createConfigurationForBroadcastTsMuxing(String videoStreamId, String audioStreamId, Double muxRate)
    {
        BroadcastTsTransportConfiguration transportConfiguration = new BroadcastTsTransportConfiguration();
        transportConfiguration.setMuxrate(muxRate);
        transportConfiguration.setStopOnError(Boolean.FALSE);
        transportConfiguration.setPatRepetitionRatePerSec(8.00);
        transportConfiguration.setPmtRepetitionRatePerSec(8.00);
        transportConfiguration.setPreventEmptyAdaptionFieldsInVideo(Boolean.TRUE);

        BroadcastProgramConfiguration programConfiguration = new BroadcastProgramConfiguration();
        programConfiguration.setProgramNumber(10);
        programConfiguration.setPidForPMT(101);
        programConfiguration.setInsertProgramClockRefOnPes(Boolean.TRUE);

        BroadcastTsVideoInputStreamConfiguration videoInputStreamMuxingConfiguration = new BroadcastTsVideoInputStreamConfiguration();
        if (videoStreamId != null) {
            videoInputStreamMuxingConfiguration.setStreamId(videoStreamId);
            videoInputStreamMuxingConfiguration.setPacketIdentifier(481);
            videoInputStreamMuxingConfiguration.setStartWithDiscontinuityIndicator(Boolean.TRUE);
            videoInputStreamMuxingConfiguration.setSetRaiOnAu(RAIUnit.ACCORDING_TO_INPUT);
            videoInputStreamMuxingConfiguration.setAlignPes(Boolean.TRUE);
            videoInputStreamMuxingConfiguration.setInsertAccessUnitDelimiterInAvc(Boolean.TRUE);
            videoInputStreamMuxingConfiguration.setMaxDecodeDelay(90000);
        }

        BroadcastTsAudioInputStreamConfiguration audioInputStreamMuxingConfiguration = new BroadcastTsAudioInputStreamConfiguration();
        if (audioStreamId != null) {
            audioInputStreamMuxingConfiguration.setStreamId(audioStreamId);
            audioInputStreamMuxingConfiguration.setLanguage("eng");
            audioInputStreamMuxingConfiguration.setPacketIdentifier(2005);
            audioInputStreamMuxingConfiguration.setStartWithDiscontinuityIndicator(Boolean.TRUE);
            audioInputStreamMuxingConfiguration.setAlignPes(Boolean.TRUE);
        }

        BroadcastTsMuxingConfiguration broadcastTsMuxingConfiguration = new BroadcastTsMuxingConfiguration();

        broadcastTsMuxingConfiguration.setTransport(transportConfiguration);
        broadcastTsMuxingConfiguration.setProgram(programConfiguration);
        if (videoStreamId != null)
            broadcastTsMuxingConfiguration.setVideoStreams(Arrays.asList(videoInputStreamMuxingConfiguration));
        if (audioStreamId != null)
            broadcastTsMuxingConfiguration.setAudioStreams(Arrays.asList(audioInputStreamMuxingConfiguration));

        return broadcastTsMuxingConfiguration;
    }

    private static String getBasefilename(String inputFilepath) {
        String[] paths = inputFilepath.split("\\/");

        String filename = paths[paths.length - 1].split("\\.")[0];

        String[] parts = filename.split("_");

        filename = StringUtils.join(Arrays.copyOfRange(parts, 0, parts.length - 1), '_');

        return filename;
    }

    private String buildBasename(String inputFilename, OutputConfig outputConfig, Integer profileId)
    {
        String filename = getBasefilename(inputFilename);

        if (profileId != null) {
            filename = String.format("%s_P%d", filename, profileId);
        } else
        {
            if (outputConfig.getVideoBitrate() != null)
            {
                filename = String.format("%s_%s_%sp_%dk", filename, outputConfig.getVideoCodec().toLowerCase(), outputConfig.getHeight(), outputConfig.getVideoBitrate() / 1000);
            }

            if (outputConfig.getAudioBitrate() != null)
            {
                filename = String.format("%s_aac_%dk", filename, outputConfig.getAudioBitrate() / 1000);
            }
        }



        return filename;
    }

    private String buildManifestPath(String type, OutputConfig outputConfig)
    {
        String basepath = type;

        if (outputConfig.getVideoBitrate() != null)
        {
            basepath = String.format("%s/video/%sp_%dk", basepath, outputConfig.getHeight(), outputConfig.getVideoBitrate() / 1000);
        }
        else if (outputConfig.getAudioBitrate() != null)
        {
            basepath = String.format("%s/audio/%dk", basepath, outputConfig.getAudioBitrate() / 1000);
        }

        return basepath;
    }

    public class EncodingConfiguration
    {
        public String jobMode;
        public EncodingMode encodingMode;
        public Boolean useCrf0;
        public Boolean reduceBitrateByPass;
        public Boolean useUnsharpFilter;
        public Boolean useDenoiseFilter;
        public Boolean useCbr;
        public List<OutputConfig> outputConfigs;
        public Boolean withAudio;

        public EncodingConfiguration(String jobMode, EncodingMode encodingMode, Boolean useCrf0,
                                     Boolean reduceBitrateByPass, Boolean useUnsharpFilter, Boolean useDenoiseFilter,
                                     Boolean useCbr, Boolean withAudio,
                                     List<OutputConfig> outputConfigs)
        {
            this.jobMode = jobMode;
            this.encodingMode = encodingMode;
            this.useCrf0 = useCrf0;
            this.useCbr = useCbr;
            this.reduceBitrateByPass = reduceBitrateByPass;
            this.useUnsharpFilter = useUnsharpFilter;
            this.useDenoiseFilter = useDenoiseFilter;
            this.outputConfigs = outputConfigs;
            this.withAudio = withAudio;
        }
    }

    public class OutputConfig
    {
        private Integer width;
        private Integer height;
        private Float frameRate;
        private Long videoBitrate;
        private Float gopSize;
        private Long audioBitrate;
        private int[] audioTracks;
        private String videoCodec;
        private String language;
        private ProfileH264 profile;
        private Integer slices;
        private Integer bFrames;
        private Boolean addQualityFilters;

        public OutputConfig(String videoCodec, ProfileH264 profile, Integer width, Integer height, Float frameRate, Long videoBitrate, Float gopSizeInSec, Integer bFrames, Integer slices, Long audioBitrate, int[] audioTracks, String language, Boolean addQualityFilters)
        {
            this.videoCodec = videoCodec;
            this.profile = profile;
            this.width = width;
            this.height = height;
            this.frameRate = frameRate;
            this.videoBitrate = videoBitrate;
            this.audioBitrate = audioBitrate;
            this.audioTracks = audioTracks;
            this.language = language;
            this.slices = slices;
            this.bFrames = bFrames;
            this.gopSize = gopSizeInSec;
            this.addQualityFilters = addQualityFilters;
        }

        public String getVideoCodec() { return videoCodec; }

        public Integer getWidth()
        {
            return width;
        }

        public void setWidth(Integer width)
        {
            this.width = width;
        }

        public Integer getHeight()
        {
            return height;
        }

        public void setHeight(Integer height)
        {
            this.height = height;
        }

        public Float getFrameRate()
        {
            return frameRate;
        }

        public void setFrameRate(Float frameRate)
        {
            this.frameRate = frameRate;
        }

        public Long getVideoBitrate()
        {
            return videoBitrate;
        }

        public void setVideoBitrate(Long videoBitrate)
        {
            this.videoBitrate = videoBitrate;
        }

        public Float getGopSize()
        {
            return gopSize;
        }

        public void setGopSize(Float gopSize)
        {
            this.gopSize = gopSize;
        }

        public Long getAudioBitrate()
        {
            return audioBitrate;
        }

        public void setAudioBitrate(Long audioBitrate)
        {
            this.audioBitrate = audioBitrate;
        }

        public int[] getAudioTracks()
        {
            return audioTracks;
        }

        public void setAudioTracks(int[] audioTracks)
        {
            this.audioTracks = audioTracks;
        }

        public String getLanguage()
        {
            return language;
        }

        public void setLanguage(String language)
        {
            this.language = language;
        }

        public ProfileH264 getProfile() { return profile; }

        public void setProfile(ProfileH264 profile) { this.profile = profile; }

        public LevelH264 getLevel()
        {
            LevelH264 level = null;
            switch (profile)
            {
                case HIGH:
                    level = LevelH264.L3_2;
                    break;

                case MAIN:
                    level = LevelH264.L3_1;
                    break;

                case BASELINE:
                    level = LevelH264.L3;
                    break;
            }

            return level;
        }

        public Integer getSlices() { return slices; }

        public Integer getBFrames() { return bFrames; }

        public Boolean addQualityFilters() {
            return addQualityFilters;
        }
    }

}
