/**
 * email.js
 * Shared nodemailer transporter and helpers for sending emails.
 * Uses env: SMTP_HOST, SMTP_PORT, EMAIL_USER, EMAIL_PASSWORD, EMAIL_SERVICE (optional).
 */

const nodemailer = require("nodemailer");

function getTransporter() {
  const user = process.env.EMAIL_USER;
  const pass = process.env.EMAIL_PASSWORD;
  const service = process.env.EMAIL_SERVICE;
  const host = process.env.SMTP_HOST;
  const port = process.env.SMTP_PORT;

  const auth = user && pass ? { user, pass } : undefined;
  if (!auth) {
    throw new Error("EMAIL_USER and EMAIL_PASSWORD are required to send email");
  }

  if (service) {
    return nodemailer.createTransport({
      service,
      auth,
    });
  }
  if (host) {
    return nodemailer.createTransport({
      host,
      port: port ? parseInt(port, 10) : 587,
      secure: port === "465",
      auth,
    });
  }
  throw new Error("Either EMAIL_SERVICE or SMTP_HOST must be set to send email");
}

/**
 * Send an email (HTML or text).
 * @param {Object} options - { to, subject, html, text }
 * @returns {Promise<void>}
 */
async function sendMail(options) {
  const from = process.env.EMAIL_USER || options.to;
  const transporter = getTransporter();
  await transporter.sendMail({
    from: `"QP Payment" <${from}>`,
    ...options,
  });
}

module.exports = {
  getTransporter,
  sendMail,
};
