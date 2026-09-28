export default function myImageLoader({ src, width, quality }) {
  return `${process.env.NEXT_PUBLIC_BASE_PATH}${src}`;
}
