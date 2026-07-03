package xyz.leeyangy.spc.service;

import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

public interface SpcDataImportExportService {

    Map<String, Object> importFromExcel(MultipartFile file, Long paramId, Long productId,
                                        Long processId, Long equipmentId, Long userId, String role);

    void exportToExcel(Long paramId, Long productId, Long equipmentId, Integer limit,
                       LocalDateTime startTime, LocalDateTime endTime,
                       HttpServletResponse response) throws IOException;

    void downloadTemplate(HttpServletResponse response) throws IOException;
}